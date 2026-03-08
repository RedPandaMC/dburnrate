# 03 — Enterprise Support: System Table Views & Governance

> Configurable table paths, column mapping, and auto-discovery for enterprise Databricks deployments where system tables are hidden behind curated views.

---

## 3.1 The Problem

Enterprise Databricks governance universally restricts direct `system.*` access to platform admins. Instead, curated views are published with row-level security, column masking, and workspace filtering:

```
governance.cost_management.v_billing_usage      → view on system.billing.usage
governance.cost_management.v_list_prices         → view on system.billing.list_prices
governance.observability.v_query_history          → view on system.query.history
governance.platform.v_compute_clusters           → view on system.compute.clusters
```

dburnrate has **8 hardcoded `system.*` references** across 3 files with zero configurability:

| File | Hardcoded Reference |
|---|---|
| `tables/billing.py:31` | `system.billing.usage` |
| `tables/billing.py:46` | `system.billing.list_prices` |
| `tables/queries.py:73` | `system.query.history` |
| `tables/queries.py:97` | `system.query.history` |
| `tables/compute.py:26` | `system.compute.node_types` |
| `tables/compute.py:44` | `system.compute.clusters` |
| `tables/compute.py:75` | `system.compute.node_timeline` |

Without a fix, dburnrate fails with permissions errors in every enterprise environment.

---

## 3.2 Solution: TableRegistry

```python
# src/dburnrate/core/table_registry.py
from dataclasses import dataclass, field

@dataclass(frozen=True)
class TableRegistry:
    """Maps logical table names to physical catalog.schema.table paths."""
    
    billing_usage: str = "system.billing.usage"
    billing_list_prices: str = "system.billing.list_prices"
    query_history: str = "system.query.history"
    compute_node_types: str = "system.compute.node_types"
    compute_clusters: str = "system.compute.clusters"
    compute_node_timeline: str = "system.compute.node_timeline"
    lakeflow_jobs: str = "system.lakeflow.jobs"
    lakeflow_job_run_timeline: str = "system.lakeflow.job_run_timeline"
    predictive_optimization: str = "system.storage.predictive_optimization_operations_history"
    
    # Column name overrides for views with different schemas
    column_overrides: dict[str, dict[str, str]] = field(default_factory=dict)
```

Usage in `tables/billing.py` changes from:
```python
# BEFORE — hardcoded
sql = f"SELECT ... FROM system.billing.usage WHERE ..."

# AFTER — registry-driven
sql = f"SELECT ... FROM {registry.billing_usage} WHERE ..."
```

---

## 3.3 Configuration Channels

### Environment Variables
```bash
export DBURNRATE_TABLE_BILLING_USAGE="governance.cost_management.v_billing_usage"
export DBURNRATE_TABLE_QUERY_HISTORY="governance.observability.v_query_history"
export DBURNRATE_TABLE_COMPUTE_CLUSTERS="governance.platform.v_compute_clusters"
```

### TOML Config (`.dburnrate.toml` or `pyproject.toml`)
```toml
[dburnrate.tables]
billing_usage = "governance.cost_management.v_billing_usage"
billing_list_prices = "governance.cost_management.v_list_prices"
query_history = "governance.observability.v_query_history"
compute_node_types = "governance.platform.v_node_types"
compute_clusters = "governance.platform.v_compute_clusters"
compute_node_timeline = "governance.platform.v_node_timeline"
```

### Programmatic API
```python
import dburnrate

registry = dburnrate.TableRegistry(
    billing_usage="governance.cost_management.v_billing_usage",
    query_history="governance.observability.v_query_history",
)
estimate = dburnrate.estimate("SELECT ...", registry=registry)
```

---

## 3.4 Column Mapping for Non-Standard Views

Governance views frequently have different schemas: pre-flattened struct columns, renamed fields, filtered/masked columns.

```python
registry = TableRegistry(
    billing_usage="governance.cost_management.v_billing_usage",
    column_overrides={
        "billing_usage": {
            "usage_metadata.cluster_id": "cluster_id",      # pre-flattened
            "usage_metadata.warehouse_id": "warehouse_id",   # pre-flattened
            "identity_metadata.run_as": "run_as_principal",  # renamed
        },
        "billing_list_prices": {
            "pricing.default": "list_price_usd",             # renamed
        },
    }
)
```

---

## 3.5 Auto-Discovery Command

```bash
dburnrate discover-tables --catalog governance --schema cost_management
```

Logic: scan `information_schema.tables` in the specified catalog/schema, match view names against known system table patterns (fuzzy match on suffixes), validate expected columns exist.

---

## 3.6 Implementation Priority

| Priority | Change | Effort |
|---|---|---|
| **P0** | `TableRegistry` dataclass with defaults | 1 hour |
| **P0** | Thread registry through billing.py, queries.py, compute.py | 2 hours |
| **P0** | Env var support in Settings | 1 hour |
| **P0** | TOML config support | 1 hour |
| **P1** | Column override mapping | 3 hours |
| **P1** | `dburnrate discover-tables` CLI | 1 day |
| **P2** | Column validation on mapped tables | 3 hours |
| **P2** | Enterprise Setup Guide documentation | 2 hours |
