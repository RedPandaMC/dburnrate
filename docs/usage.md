# dburnrate — Usage Guide

> Pre-execution cost estimation for Databricks. Know what a query costs **before** you run it.

---

## Installation

```bash
# Clone and install
git clone https://github.com/your-org/dburnrate
cd dburnrate
uv sync

# Verify
uv run dburnrate --help
```

---

## Quick Start

### Estimate a SQL query (static, no connection required)

```bash
uv run dburnrate estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"
```

Output:
```
        Cost Estimate
┌─────────────────┬───────────┐
│ Metric          │ Value     │
├─────────────────┼───────────┤
│ Estimated DBU   │ 0.0312    │
│ Estimated Cost  │ $0.0062   │
│ Confidence      │ medium    │
│ Signal          │ static    │
└─────────────────┴───────────┘
```

### Estimate a .sql file

```bash
uv run dburnrate estimate ./queries/daily_revenue.sql
```

### Estimate a Jupyter notebook (SQL cells only)

```bash
uv run dburnrate estimate ./notebooks/analysis.ipynb
```

---

## Cluster Configuration

```bash
# Specify instance type and worker count
uv run dburnrate estimate "SELECT ..." \
  --cluster Standard_DS5_v2 \
  --workers 8

# Enable Photon
uv run dburnrate estimate "SELECT ..." --photon

# Output in EUR
uv run dburnrate estimate "SELECT ..." --currency EUR
```

**Available instance types** (Azure, more in `src/dburnrate/core/pricing.py`):

| Instance | DBU/hr |
|----------|--------|
| Standard_DS3_v2 | 0.75 |
| Standard_DS4_v2 | 1.50 |
| Standard_DS5_v2 | 3.00 |
| Standard_L8s_v3 | 2.00 |

---

## Hybrid Estimation (with Databricks connection)

When you connect to your workspace, dburnrate uses **EXPLAIN COST** and **query history** for significantly more accurate estimates.

### Setup

```bash
export DBURNRATE_WORKSPACE_URL=https://adb-1234567890.12.azuredatabricks.net
export DBURNRATE_TOKEN=dapi...
```

Or create a `.env` file:
```ini
DBURNRATE_WORKSPACE_URL=https://adb-1234567890.12.azuredatabricks.net
DBURNRATE_TOKEN=dapi...
```

### Run with EXPLAIN COST

```bash
uv run dburnrate estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1" \
  --warehouse-id sql-warehouse-abc123
```

This will:
1. Fingerprint your SQL and look up matching historical executions
2. Run `EXPLAIN COST` on the query (no data scanned)
3. Blend historical p50, EXPLAIN statistics, and static analysis
4. Return a confidence=**high** estimate

### Signal priority

| Signal available | Confidence | Method |
|-----------------|-----------|--------|
| Historical match (≥1 past execution) | **high** | p50 median duration |
| EXPLAIN COST with full stats | **high** | 70% EXPLAIN + 30% static |
| EXPLAIN COST with partial stats | **medium** | 40% EXPLAIN + 60% static |
| Static only | **low/medium** | SQL complexity scoring |

---

## What-If Scenarios

Model the cost impact of infrastructure changes:

```bash
# How much cheaper would Photon make this?
uv run dburnrate whatif "SELECT ..." --scenario photon

# What if we migrate to serverless SQL?
uv run dburnrate whatif "SELECT ..." --scenario serverless --utilization 60

# Resize cluster
uv run dburnrate whatif "SELECT ..." --scenario resize
```

---

## Output Formats

```bash
# Rich table (default)
uv run dburnrate estimate "SELECT ..."

# JSON (pipe to jq, CI/CD)
uv run dburnrate estimate "SELECT ..." --output json | jq .

# Plain text
uv run dburnrate estimate "SELECT ..." --output text
```

---

## Anti-pattern Detection

dburnrate automatically flags expensive query patterns:

| Anti-pattern | Severity | Example |
|-------------|---------|---------|
| CROSS JOIN | warning | `FROM a, b` without ON |
| SELECT * without LIMIT | info | `SELECT * FROM large_table` |
| ORDER BY without LIMIT | warning | Forces global sort |
| collect() without limit() | error | OOM risk on driver |
| Python UDF | warning | Use @pandas_udf instead |
| repartition(1) | warning | Single partition bottleneck |
| toPandas() | warning | Brings all data to driver |

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DBURNRATE_WORKSPACE_URL` | Databricks workspace URL | — |
| `DBURNRATE_TOKEN` | Personal access token | — |
| `DBURNRATE_TARGET_CURRENCY` | Output currency | `USD` |
| `DBURNRATE_PRICING_SOURCE` | `embedded` or `live` | `embedded` |

---

## Programmatic API

```python
from dburnrate.core.config import Settings
from dburnrate.core.models import ClusterConfig
from dburnrate.estimators.hybrid import HybridEstimator
from dburnrate.parsers.explain import parse_explain_cost
from dburnrate.tables.connection import DatabricksClient
from dburnrate.tables.queries import fingerprint_sql, find_similar_queries

# Build cluster config
cluster = ClusterConfig(
    instance_type="Standard_DS4_v2",
    num_workers=4,
    dbu_per_hour=1.5,
)

# Static estimate (no connection)
from dburnrate.estimators.static import CostEstimator
estimator = CostEstimator(cluster=cluster)
result = estimator.estimate("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
print(f"Cost: ${result.estimated_cost_usd:.4f} ({result.confidence} confidence)")

# Hybrid estimate (with connection)
settings = Settings()
with DatabricksClient(settings) as client:
    # Look up history
    fingerprint = fingerprint_sql(sql)
    historical = find_similar_queries(client, fingerprint, warehouse_id="sql-abc")

    # Run EXPLAIN COST
    rows = client.execute_sql(f"EXPLAIN COST {sql}", "sql-abc")
    plan = parse_explain_cost(rows[0]["plan"])

    # Blend signals
    hybrid = HybridEstimator()
    result = hybrid.estimate(sql, cluster, explain_plan=plan, historical=historical)
    print(f"Cost: ${result.estimated_cost_usd:.4f} ({result.confidence} confidence)")
```

---

## Architecture

```
Query / File / Notebook
        │
        ▼
   SQL Parser (sqlglot)          ─── Anti-pattern detector
        │
        ├──── Static Estimator ────────────────────────────┐
        │     complexity score → DBU                       │
        │                                                  ▼
        ├──── EXPLAIN COST Parser ──► HybridEstimator ──► CostEstimate
        │     sizeInBytes, rowCount,   weighted blend      confidence
        │     join types, shuffles                         estimated_dbu
        │                                                  estimated_cost_usd
        └──── Query History Lookup
              fingerprint → p50 duration
```

---

## Development

```bash
# Run tests
uv run pytest -m unit -v

# Lint
uv run ruff check src/ tests/

# Format
uv run ruff format src/ tests/

# Security scan
uv run bandit -c pyproject.toml -r src/
```
