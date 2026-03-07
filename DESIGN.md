# dburnrate Design Document

> Comprehensive design document consolidating research, architecture, and roadmap

---

## Table of Contents

1. [Research Findings](#research-findings)
2. [Architecture & Concepts](#architecture--concepts)
3. [Implementation Roadmap](#implementation-roadmap)
4. [Verification & Quality](#verification--quality)

---

# Research Findings

## Overview

**Databricks has no BigQuery-style dry-run API, but a powerful hybrid architecture is achievable** by combining Spark's `EXPLAIN COST` plans, Delta Lake transaction log metadata, query fingerprinting against `system.query.history`, and ML models trained on execution plan features. The critical insight from researching how Netflix, Uber, and Airbnb handle this at scale: no organization does pure pre-execution cost estimation from scratch — every production system relies on a blend of static plan analysis for cold-start queries and historical execution data for recurring workloads.

---

## EXPLAIN COST provides rich statistics without execution

Spark SQL's `EXPLAIN COST <query>` generates the full optimized logical plan with **per-operator `sizeInBytes` and `rowCount` estimates** without reading a single byte of data. This is the strongest pre-execution signal available on the platform.

**Five EXPLAIN modes:**
- `SIMPLE` - Basic plan structure
- `EXTENDED` - Logical and physical plans
- `COST` - **Key mode for estimation** with statistics
- `CODEGEN` - Generated code
- `FORMATTED` - Machine-readable JSON

**Typical output:**
```
Aggregate [s_store_sk], Statistics(sizeInBytes=20.0 B, rowCount=1)
+- Join Inner, Statistics(sizeInBytes=30.8 MB, rowCount=1.62E+6)
   +- Relation parquet, Statistics(sizeInBytes=134.6 GB, rowCount=2.88E+9)
```

**Physical plan reveals:**
- Join strategies (BroadcastHashJoin vs SortMergeJoin vs ShuffledHashJoin)
- Shuffle operations
- Pushed-down filters
- AQE markers

**Programmatic access:**
- `spark.sql("EXPLAIN COST ...")` returns DataFrame
- `df._jdf.queryExecution().optimizedPlan().stats()` exposes structured stats

**Accuracy caveats:**
- With `ANALYZE TABLE` statistics: ~1.5× error factor
- Without statistics: **1000× or more error**
- Databricks Runtime 16.0+ reports statistics completeness: `missing`/`partial`/`full`
- Predictive Optimization (GA 2025) auto-runs ANALYZE on UC tables

**Comparison to competitors:**
- **BigQuery**: `dryRun: true` → exact `totalBytesProcessed` → direct dollar cost at $5/TB
- **Snowflake**: EXPLAIN shows partition pruning but no dollar cost; must retroactively compute from QUERY_HISTORY
- **Databricks**: Richest plan detail but **no direct DBU-to-dollar mapping** from EXPLAIN

---

## Delta transaction logs enable exact scan-size estimation

Delta Lake's `_delta_log` stores per-file metadata providing **exact** table-level statistics without data scanning.

**Per-file statistics in `add` actions:**
- `numRecords` - exact row count
- `minValues`/`maxValues` - per-column (first 32 columns)
- `nullCount` - per-column
- `size` - exact file size in bytes

**Access methods:**
- `DESCRIBE DETAIL tablename` → `sizeInBytes`, `numFiles`, `partitionColumns`, `lastModified`
- `delta-rs` library: `get_add_actions(flatten=True)` returns DataFrame with per-file stats

**Two statistics systems:**
1. **Delta data-skipping** - automatic, always current (file-level min/max/null/record counts)
2. **Query optimizer statistics** - requires `ANALYZE TABLE`, stored in metastore (distinct_count, histograms, avg_col_len)

**Practical implications:**
- Use `DESCRIBE DETAIL` for instant table-level size
- Parse `add` actions for partition-level sizes
- File-level min/max for predicate filter estimation
- Reserve ANALYZE statistics for join cardinality

---

## Query fingerprinting turns history into prediction

**Normalization pipeline** (Percona `pt-fingerprint` pattern):
1. Strip comments
2. Normalize whitespace
3. Replace literals with `?`
4. Collapse IN-lists to single placeholders
5. Abstract database names
6. SHA-256 hash → `template_id`

**`system.query.history` metrics:**
- `total_duration_ms`
- `read_bytes`, `read_rows`, `read_files`
- `produced_rows`
- `spill_to_disk_bytes`
- `total_task_duration_ms`
- Cache hit indicators

**Cost attribution:** `(query_duration / total_warehouse_duration_in_hour) × hourly_DBU_cost × list_price`

**Three similarity-matching tiers:**
1. **Exact fingerprint match** (highest confidence) → historical p50/p95
2. **AST edit distance** (high confidence) → SQLGlot structural distance
3. **Embedding-based similarity** (medium confidence) → CodeBERT/`all-mpnet-base-v2` vectors

**Uber's production system:** 100K+ Spark applications/day using Spark Event Listeners → Kafka → Flink analyzers with 180-day windows.

---

## ML models achieve 14–98% accuracy

**Microsoft's Cleo (SIGMOD 2020):**
- Traditional models with perfect cardinalities: **258% median error**, 0.04 Pearson correlation
- Learned approach: **14% median error** (operator-subgraph), **42%** (operator-level)
- Architecture: large collection of smaller specialized models with meta-model ensemble

**Twitter (IC2E 2021):**
- Raw SQL text features, classification into resource buckets
- **97.9% accuracy** CPU prediction, **97%** memory
- ~200ms inference time

**RAAL/DRAL (ICDE 2022-2024):**
- First Spark-specific learned cost models
- Key innovation: resource-awareness (executors, memory, cores as features)
- DRAL adds data-aware features via unsupervised learning

**Cold-start solutions:**
- Zero-shot models (Hilprecht & Binnig, VLDB 2022): graph neural networks with transferable features
- Few-shot fine-tuning: 10–100 queries dramatically improves accuracy
- Bao (SIGMOD 2021): Thompson sampling for exploration-exploitation

**Counterpoint (Heinrich et al., SIGMOD 2025):** Traditional PostgreSQL cost models often outperform learned models on actual plan selection despite higher estimation errors.

---

## Unity Catalog metadata gaps

**What's missing:**
- `information_schema.tables`: no size, row count, or statistics
- `information_schema.columns`: no cardinality estimates, distribution info, or null counts
- Unity Catalog REST API: no size or statistics

**No `pg_class`-equivalent** for passive table size recording across all tables.

**Metadata access hierarchy:**
1. `DESCRIBE DETAIL` → best lightweight source
2. `DESCRIBE TABLE EXTENDED ... AS JSON` → statistics if ANALYZE/PO ran
3. `information_schema.columns` + type-based width estimation
4. `system.storage.predictive_optimization_operations_history`
5. Lakehouse Monitoring profile tables (richest, requires setup)

---

## Serverless billing challenges

**Billing model:** DBU/hour × uptime (per-second granularity), **not per-query**

**Fixed burn rates:**
- X-Small = 6 DBU/hr
- Small = 12 DBU/hr
- etc.

**Per-DBU rate:** ~$0.70 (AWS US) - bundles infrastructure

**No pre-execution cost estimation API exists** in Databricks platform. IWM system predicts internally for autoscaling but doesn't expose predictions.

**Viable approximation:** duration-proportional attribution via `system.query.history` + `system.billing.usage` join on `warehouse_id` + time overlap.

---

## Hybrid architecture for pre-execution estimation

**Tier 1 — Instant metadata lookup (< 100ms):**
- Cached `DESCRIBE DETAIL` results
- Compute total input data volume
- Partition pruning estimates

**Tier 2 — EXPLAIN plan analysis (< 2 seconds):**
- Submit `EXPLAIN COST <query>` via Statement Execution API
- Parse for `sizeInBytes`/`rowCount`, join strategies, shuffle count
- Cross-validate with Tier 1 Delta metadata

**Tier 3 — Historical fingerprint matching (< 500ms):**
- Normalize and hash → `template_id`
- Look up in `system.query.history` index
- Exact matches: p50/p95 with confidence intervals
- Near-matches: AST edit distance or embedding similarity

**Tier 4 — ML model prediction (< 300ms):**
- Features: operator types, cardinalities from EXPLAIN, table sizes, cluster config
- Classify into cost buckets (low/medium/high/very-high) per Twitter approach
- Cleo's hierarchical strategy: specialized → general → zero-shot

**Compute type differentiation:**
- **SQL Warehouses:** cost ∝ query duration × fixed DBU rate
- **Jobs Compute:** cost = cluster_uptime × instance_cost + DBU_cost
- **Interactive clusters:** resource share via `total_task_duration_ms` / cluster capacity

---

# Architecture & Concepts

## Static analysis turns SQL and PySpark into cost signals

**SQLGlot (v28.10.1)** — SQL parsing backbone:
- Databricks officially supported dialect
- First-class support: `MERGE INTO`, `COPY INTO`, `CREATE STREAMING TABLE`, JSON colon extraction (`col:path`), `CLUSTER BY`, Delta DML
- Typed expression nodes: `exp.Join`, `exp.AggFunc`, `exp.Window`, `exp.Merge`
- Zero dependencies, Python 3.9+, Rust-accelerated tokenizer available
- Limitations: `OPTIMIZE`/`ZORDER BY` parse as generic `Command`

**Python `ast` module** — PySpark analysis:
- Custom `ast.NodeVisitor` detects: `.groupBy()`, `.join()`, `.collect()`, `.toPandas()`, `.repartition()`
- Decorator patterns: `@udf` vs `@pandas_udf`
- Embedded SQL via `spark.sql()` strings → route to SQLGlot
- No off-the-shelf PySpark anti-pattern detector exists

**Anti-patterns to flag:**
- `collect()` without prior `limit()`
- Python UDFs instead of Pandas UDFs (10–100× overhead)
- `.crossJoin()`
- `.repartition(1)`
- `toPandas()` on unbounded datasets

**Notebook parsing:**
- `.ipynb`: JSON with cell schema (via `nbformat` or raw `json`)
- `.dbc`: ZIP with JSON notebooks (UTF-8, Base64, or gzip-compressed)
- Magic commands: `%sql`, `%python`, `%scala`
- Source exports: `# COMMAND ----------` delimiters

---

## Complexity scoring model

| Operation | Weight | Rationale |
|-----------|--------|-----------|
| `MERGE INTO` | 20 | Join + scan + file rewrite |
| Cross join | 50 | O(n×m) output |
| Shuffle join | 10 | Full data redistribution |
| `GROUP BY` | 8 | Shuffle for aggregation |
| Window function | 8 | Shuffle + sort within partitions |
| `collect()` / `toPandas()` | 25 | Driver memory anti-pattern |
| Python UDF | 15 | Row-at-a-time JVM↔Python serialization |
| Pandas UDF | 5 | Vectorized via Arrow |
| `ORDER BY` | 7 | Global sort requires shuffle |
| `DISTINCT` | 6 | Dedup shuffle |

**DBU cost formula:** `complexity_score × data_volume_factor × cluster_config_factor × dbu_rate`

---

## System tables reference

**`system.billing.usage`** (GA, 365-day retention):
- `usage_quantity` in DBUs
- `sku_name` for compute type
- `usage_metadata`: `cluster_id`, `warehouse_id`, `job_id`, `job_run_id`, `notebook_id`, `dlt_pipeline_id`, `endpoint_name`
- `billing_origin_product`: `JOBS`, `SQL`, `INTERACTIVE`, `MODEL_SERVING`, `PREDICTIVE_OPTIMIZATION`
- `identity_metadata.run_as` for per-user attribution
- `custom_tags` for team/department attribution

**`system.billing.list_prices`** (GA, indefinite retention):
- Historical pricing by SKU, cloud, time period
- Join: `usage_quantity × pricing.effective_list.default`

**`system.query.history`** (Public Preview, 365-day retention):
- `statement_text`, `execution_duration_ms`, `total_task_duration_ms`
- `read_bytes`, `read_rows`, `spilled_local_bytes`, `network_sent_bytes`, `written_bytes`
- `query_source` links to notebooks, jobs, dashboards
- No direct join to billing.usage — correlate via `warehouse_id`/`notebook_id`/`job_id` + time window

**Additional tables (2024-2025):**
- `system.lakeflow.*` - job and pipeline monitoring (June 2024)
- `system.compute.node_timeline` - minute-by-minute utilization (90-day retention)
- `system.serving.endpoint_usage` - token-level model serving
- `system.storage.predictive_optimization_operations_history` - COMPACTION/VACUUM/CLUSTERING with DBU
- `system.mlflow.*` - experiment tracking
- `system.compute.node_types` - instance type → hardware specs mapping

---

## Azure pricing model

**Dual-bill:** DBUs for platform + Azure VMs for infrastructure (except serverless)

**Premium tier, US East, pay-as-you-go:**

| Compute type | $/DBU | Includes VM? |
|-------------|-------|-------------|
| Jobs Compute (Classic) | **$0.30** | No |
| Jobs Light | $0.22 | No |
| All-Purpose (Classic) | **$0.55** | No |
| Serverless Jobs | **$0.45** | Yes (50% promo) |
| Serverless Notebooks | **$0.95** | Yes (30% promo) |
| SQL Classic | $0.22 | No |
| SQL Pro | $0.55 | No |
| SQL Serverless | **$0.70** | Yes |
| DLT Core/Pro/Advanced | $0.30 / $0.38 / $0.54 | No |
| Model Serving (CPU/GPU) | $0.07 | Yes |

**Instance DBU rates:**
- Standard_DS3_v2 (4 vCPU, 14 GiB) = 0.75 DBU/hr
- Standard_DS4_v2 (8 vCPU, 28 GiB) = 1.50 DBU/hr
- Standard_D64s_v3 (64 vCPU, 256 GiB) = 12.0 DBU/hr

**Photon:**
- Classic clusters: **2.5× DBU multiplier** (Azure), 2.9× (AWS)
- SQL Warehouses/serverless: enabled by default, no extra charge
- Breakeven: **2× runtime speedup** - benchmarks show 2.7× average for complex SQL

**Serverless:**
- Eliminates VM management
- SQL Warehouse sizes: Small = 12 DBU/hr = $8.40/hr all-in
- Steady-state: 2–5× more than optimized classic, but eliminates idle costs

**Committed-use discounts:**
- 1-year: up to **33% savings**
- 3-year: up to **37% savings**

---

## Forecasting architecture

**Prophet** (default):
- Daily/weekly/yearly seasonality
- Holiday calendars
- Trend changepoints
- Bayesian uncertainty intervals (`yhat_lower`, `yhat_upper`)

**NeuralProphet** (optional upgrade):
- 55–92% accuracy improvement over Prophet
- Conformalized Quantile Regression (CQR) for guaranteed coverage

**Implementation decisions:**
- Train per SKU × workspace
- Normalize for business events as explicit regressors
- Handle regime changes (cluster resize, serverless migration) as changepoints
- Target <15% MAPE; best-in-class: <10% overrun frequency, <20% deviation

**Optional dependency:** `pip install dbcost[forecasting]` (~200 MB for Prophet's `cmdstanpy`)

---

## What-if scenarios

**Photon toggle:**
```
cost = (current_DBU × 2.5) / speedup_factor
```
- Complex joins: 2–4× speedup (20–57% net cost reduction)
- Aggregations: 4× speedup
- Simple inserts: zero speedup, **72% cost increase**

**Liquid Clustering migration:**
- **2–12× read performance improvement**
- **7× faster writes** vs partitioning + Z-ORDER
- Incremental rewrites vs full partition rewrites
- Migrate when partition cardinality >5,000 or patterns evolve

**Cluster right-sizing:**
- **CPU-bound** (>90% util, heavy UDFs) → scale cores, C-series
- **Memory-bound** (spill, OOM) → scale memory, E/R-series
- **I/O-bound** (slow reads, small files) → fix data layout, then L-series
- **Shuffle-bound** (high network) → scale out

**Savings:**
- Fixed → autoscaling: **40–60%**
- Spot instances: additional **60–80%** on VM costs

**Serverless migration:**
- Compare: `classic_cost = DBU_rate × DBU_count + VM_cost`
- vs: `serverless_cost = serverless_DBU_rate × estimated_DBU_count`
- Breakeven: <30% active compute time typically benefits from serverless

---

## Competitive landscape

**Databricks native:**
- System tables, AI/BI dashboards, Budgets API
- Cloud Infra Cost Field Solution (open-source)
- Overwatch (deprecated — system tables subsumed)

**Open-source:**
- SparkMeasure (v0.25): collects task/stage metrics, no dollar translation
- OpenCost: Databricks plugin (early-stage, KubeCon 2024)
- Onehouse Spark Analyzer (2025): Excel reports from History Server

**Commercial:**
- **Unravel Data**: AI-powered optimization, CI/CD integration, 25–35% savings
- **Sedai** (GA June 2025): autonomous right-sizing via RL
- **Sync Computing/Slingshot**: per-job ML models, 37–55% savings
- **Acceldata**: cost + quality observability, 96–98% forecast accuracy
- **CloudZero**: multi-vendor aggregation, lacks DB-specific optimization

**Gap:** No tool provides **pre-execution cost estimation from code**, **what-if modeling with quantified tradeoffs**, or a **reusable Python library** for notebooks/CI/CD.

---

## Package architecture

```
dburnrate/
├── src/
│   └── dburnrate/
│       ├── __init__.py
│       ├── _compat.py              # Optional import helpers
│       ├── core/
│       │   ├── models.py           # Pydantic models
│       │   ├── config.py           # Pydantic-settings
│       │   ├── pricing.py          # DBU rate lookups
│       │   └── protocols.py        # Protocol classes
│       ├── parsers/
│       │   ├── sql.py              # SQLGlot analysis [sql]
│       │   ├── pyspark.py          # AST analysis
│       │   ├── notebooks.py        # .ipynb + .dbc
│       │   └── antipatterns.py     # Detection rules
│       ├── tables/
│       │   ├── billing.py          # system.billing.*
│       │   ├── queries.py          # system.query.history
│       │   ├── compute.py          # system.compute.*
│       │   └── attribution.py     # Cost joins
│       ├── estimators/
│       │   ├── static.py           # Complexity-based
│       │   ├── historical.py       # History-based
│       │   ├── hybrid.py           # Combined approach
│       │   ├── self.py             # Self-referential [NEW]
│       │   └── whatif.py           # Scenarios
│       ├── batch.py                # Batch file analysis [NEW]
│       ├── cli/
│       │   └── main.py             # Typer entry point
│       └── forecast/
│           └── prophet.py          # Time-series [forecasting]
├── tests/
├── pyproject.toml
└── README.md
```

**pyproject.toml extras:**
```toml
[project.optional-dependencies]
sql = ["sqlglot>=20.0"]
forecasting = ["prophet>=1.1"]
ml = ["scikit-learn>=1.3"]
all = ["dburnrate[sql,forecasting,ml]"]
```

**Type safety:**
- Python 3.12+: PEP 695 generics, Protocol classes, TypedDict
- pydantic-settings with env vars: `DBCOST_WORKSPACE_URL`, `DBCOST_TOKEN`

**Graceful imports:**
```python
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlglot

def _require(module: str, extra: str):
    try:
        return __import__(module)
    except ImportError:
        raise ImportError(f"Install with: pip install dburnrate[{extra}]") from None
```

---

## Design principles

**Layered estimation fidelity:**
1. Static analysis (code only) — cheapest, useful for dev/CI
2. Historical data enrichment — medium accuracy
3. Live pricing + forecasting — highest accuracy, requires runtime context

**Single most impactful capability:** Pre-execution cost estimation answering "what will this notebook cost?" before cluster starts.

---

# Implementation Roadmap

## Phase 1: Foundation & Validation (Critical)

### 1.1 Run and Fix Tests ✅
- [x] Run existing tests: `uv run pytest -m unit -v`
- [x] Run linting: `uv run ruff check src/ tests/`
- [x] Run formatting check: `uv run ruff format --check src/ tests/`
- [x] Fix all linting/type errors
- [ ] Add return type hints to all functions
- [ ] Add missing docstrings
- [ ] Run security audit: `uv run bandit -c pyproject.toml -r src/`

### 1.2 Code Quality
- [ ] Type hints on all public functions
- [ ] Proper exception handling
- [ ] CLI argument validation

---

## Phase 2: System Tables Integration (High Priority)

### 2.1 Billing Integration
- [ ] Implement `src/dburnrate/tables/billing.py`
- [ ] Query `system.billing.usage`
- [ ] Query `system.billing.list_prices`
- [ ] Cost attribution: `query_duration / total_duration * hourly_cost`

### 2.2 Query History Integration
- [ ] Implement `src/dburnrate/tables/queries.py`
- [ ] Extract: `execution_duration_ms`, `read_bytes`, `read_rows`, `total_task_duration_ms`

### 2.3 Compute Integration
- [ ] Implement `src/dburnrate/tables/compute.py`
- [ ] Query `system.compute.node_types`
- [ ] Query `system.compute.clusters`
- [ ] Query `system.compute.node_timeline`

### 2.4 Databricks Connection
- [ ] REST API client
- [ ] Workspace URL + token auth
- [ ] Connection pooling and retry logic
- [ ] Rate limiting

---

## Phase 3: EXPLAIN COST Integration (High Priority)

### 3.1 EXPLAIN Parsing
- [ ] Implement `src/dburnrate/parsers/explain.py`
- [ ] Parse `sizeInBytes` and `rowCount`
- [ ] Extract join strategies
- [ ] Count shuffle operations

### 3.2 Statistics Handling
- [ ] Detect completeness (missing/partial/full)
- [ ] Handle tables without ANALYZE
- [ ] Delta metadata integration

### 3.3 Hybrid Estimation
- [ ] Combine static + EXPLAIN
- [ ] Weight EXPLAIN higher when stats available
- [ ] Confidence boost on agreement

---

## Phase 4: Delta Metadata Integration (Medium Priority)

### 4.1 Delta Log Parsing
- [ ] `_delta_log` parsing
- [ ] Extract: `numRecords`, `size`, `minValues`, `maxValues`, `nullCount`
- [ ] `DESCRIBE DETAIL` wrapper

### 4.2 Scan Size Estimation
- [ ] Total table size from transaction log
- [ ] Partition-filtered scan sizes
- [ ] Data volume factor

### 4.3 Data Skipping
- [ ] File-level min/max for predicate estimation
- [ ] Z-ORDER/Liquid Clustering effectiveness

---

## Phase 5: Historical Fingerprinting (High Priority)

### 5.1 Query Normalization
- [ ] Percona-style normalization
- [ ] Replace literals with `?`
- [ ] Collapse IN-lists
- [ ] Abstract database/schema names

### 5.2 Template Matching
- [ ] SHA-256 template hashing
- [ ] Exact match → historical p50/p95
- [ ] Cache template → cost mappings

### 5.3 Similarity Matching
- [ ] AST edit distance (sqlglot)
- [ ] Embedding-based similarity (CodeBERT)

---

## Phase 6: ML Cost Models (Medium Priority)

### 6.1 Feature Extraction
- [ ] Operator types
- [ ] Cardinalities from EXPLAIN
- [ ] Table sizes from Delta
- [ ] Cluster configuration

### 6.2 Classification Model
- [ ] Cost buckets: low/medium/high/very-high
- [ ] sklearn implementation
- [ ] Training data from history

### 6.3 Advanced Models
- [ ] RAAL/DRAL-inspired models
- [ ] Zero-shot transfer

---

## Phase 7: Multi-Cloud Support (Medium Priority)

### 7.1 AWS Support
- [ ] AWS DBU rates
- [ ] AWS instance types
- [ ] Photon multiplier (2.9x)

### 7.2 GCP Support
- [ ] GCP DBU rates
- [ ] GCP instance types

### 7.3 Refactoring
- [ ] Cloud selection in pricing.py
- [ ] Cloud detection from workspace URL

---

## Phase 8: Enhanced Operations Detection (Medium Priority)

### 8.1 SQL Operations
- [ ] COPY INTO detection
- [ ] OPTIMIZE/ZORDER detection
- [ ] Streaming table detection
- [ ] Liquid Clustering detection

### 8.2 Unity Catalog
- [ ] Catalog/schema awareness
- [ ] 3-level naming
- [ ] Metastore integration

### 8.3 Anti-patterns
- [ ] Expand detection
- [ ] Severity levels
- [ ] Suggestions

---

## Phase 9: Production Hardening (High Priority)

### 9.1 Error Handling
- [ ] Comprehensive exceptions
- [ ] User-friendly messages
- [ ] Recovery strategies

### 9.2 Performance
- [ ] Metadata lookup caching
- [ ] Connection pooling
- [ ] Batch queries

### 9.3 Observability
- [ ] Structured logging
- [ ] Metrics collection
- [ ] Debug mode

---

## Phase 10: CLI Enhancements (Low Priority)

### 10.1 Commands
- [ ] `--warehouse-id` flag
- [ ] `--job-id` flag
- [ ] `--export` JSON/CSV
- [ ] `--watch` continuous monitoring
- [ ] Configuration file support

### 10.2 Output
- [ ] Table visualization
- [ ] Comparison output
- [ ] Trend charts

---

## Phase 11: Self-Referential Cost Estimation (NEW)

> Enable the package to estimate cost of code currently being executed

### 11.1 Core API
- [ ] Implement `dburnrate.estimate_self()` function
- [ ] Read current file via `__file__` or `inspect`
- [ ] Parse all code above the import statement
- [ ] Return CostEstimate for current module

### 11.2 Usage Pattern
```python
# At the bottom of any Python file/notebook
import dburnrate
estimate = dburnrate.estimate_self()
print(f"This file would cost ${estimate.cost_usd:.4f} to run")
```

### 11.3 Implementation Details
- [ ] Detect if running in notebook vs script
- [ ] Handle Jupyter magic commands
- [ ] Exclude import statement itself from analysis
- [ ] Cache results to avoid re-parsing

### 11.4 CLI Support
- [ ] `dburnrate estimate-self` command
- [ ] Auto-detect current working file
- [ ] Support for specific file path override

---

## Phase 12: Batch File Analysis (NEW)

> Analyze multiple files at once with glob pattern support

### 12.1 Core API
- [ ] Implement `dburnrate.estimate_batch()` function
- [ ] Support glob patterns: `queries/*.sql`
- [ ] Support explicit file lists: `['query1.sql', 'query2.py']`
- [ ] Support directory recursion

### 12.2 Output Formats
- [ ] Summary table with aggregate stats
- [ ] Individual results per file
- [ ] CSV export option
- [ ] JSON export option

### 12.3 CLI Commands
```bash
# Glob patterns
dburnrate estimate-batch "queries/*.sql"
dburnrate estimate-batch "notebooks/**/*.ipynb"

# Directories (recursive by default)
dburnrate estimate-batch ./queries/

# Multiple specific files
dburnrate estimate-batch query1.sql query2.py notebook.ipynb

# Export options
dburnrate estimate-batch queries/ --format csv --output costs.csv
dburnrate estimate-batch queries/ --format json --output costs.json
```

### 12.4 Features
- [ ] Parallel processing for large batches
- [ ] Progress bar with `rich`
- [ ] Skip unsupported file types gracefully
- [ ] Aggregate statistics (total cost, average, min/max)
- [ ] Sort by cost (highest first)
- [ ] Filter by confidence level

---

## Phase 13: CLI Workflows & Documentation (NEW)

> Comprehensive CLI usage patterns and CI/CD integration

### 13.1 Common Workflows
- [ ] Document single-file estimation workflow
- [ ] Document batch analysis workflow
- [ ] Document comparison workflow (what-if scenarios)
- [ ] Document integration with Git workflows

### 13.2 CI/CD Integration
- [ ] GitHub Actions workflow template
- [ ] GitLab CI template
- [ ] Pre-commit hook for cost estimation
- [ ] PR comment bot (post estimates on PRs)

### 13.3 Documentation
- [ ] `docs/cli-workflows.md` - Complete CLI guide
- [ ] Usage examples for common scenarios
- [ ] Troubleshooting guide
- [ ] Performance optimization tips

### 13.4 GitHub Actions Example
```yaml
# .github/workflows/cost-check.yml
name: Cost Estimation Check
on: [pull_request]
jobs:
  estimate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Estimate costs
        run: |
          pip install dburnrate[sql]
          dburnrate estimate-batch queries/ --format json --output costs.json
      - name: Comment PR
        uses: actions/github-script@v7
        with:
          script: |
            const costs = require('./costs.json');
            // Post cost summary as PR comment
```

---

## Post-MVP: Advanced Features

### Forecasting
- [ ] Prophet-based cost forecasting
- [ ] Per-SKU × workspace models
- [ ] Business event awareness
- [ ] Confidence intervals

### DLT/SDP
- [ ] DLT pipeline cost estimation
- [ ] Tier detection (Core/Pro/Advanced)
- [ ] Pipeline dependency analysis

### Cluster Optimization
- [ ] Right-sizing recommendations
- [ ] Bottleneck classification
- [ ] Instance family recommendations

---

# Verification & Quality

## Required Commands

Run after each phase:

```bash
# Unit tests
uv run pytest -m unit -v

# Coverage
uv run pytest --cov --cov-report=term-missing

# Lint
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/

# Docstring coverage
uv run interrogate src/ -v

# Security audit
uv run pip-audit
```

## Key Documents

- **DESIGN.md** (this file) - Research, architecture, and roadmap
- **AGENTS.md** - LLM working rules and workflow
- **PLAN.md** - Original implementation plan
- **README.md** - User-facing documentation

---

## Design Principles Summary

1. **Layered fidelity** - Static → Historical → Live pricing
2. **Hybrid architecture** - EXPLAIN + Delta + fingerprinting + ML
3. **Multi-signal approach** - No single silver bullet
4. **Self-referential** - Package can estimate its own cost
5. **Batch analysis** - Glob patterns for bulk operations
6. **CI/CD native** - Built for automation and workflows

*Document version: 1.0 | Last updated: March 2026*
