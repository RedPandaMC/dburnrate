<div align="center">

<img src="public/logo_text.svg" alt="dburnrate" width="400">

**Pre-execution cost estimation for Databricks**

Know what a query costs _before_ you run it.

[![Tests](https://img.shields.io/badge/tests-263%20passing-brightgreen)](https://github.com/anomalyco/dburnrate/actions)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![Ruff](https://img.shields.io/badge/lint-ruff-purple)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![PyPI](https://img.shields.io/badge/pypi-coming%20soon-orange)](https://pypi.org)

</div>

---

## What is this?

`dburnrate` estimates the DBU cost of a Databricks SQL query or PySpark notebook **without executing it**. It combines three signals ranked by accuracy:

| Signal | How | Accuracy |
|--------|-----|----------|
| **Historical match** | SHA-256 fingerprint → `system.query.history` p50 | ★★★★★ |
| **EXPLAIN COST** | Optimizer stats: scan size, join types, shuffles | ★★★★☆ |
| **Static analysis** | SQL complexity scoring via sqlglot | ★★★☆☆ |

When a Databricks connection is available, all three signals are blended by `HybridEstimator`. Without a connection, the static estimator works offline on any SQL or PySpark code.

---

## Install

```bash
git clone https://github.com/your-org/dburnrate
cd dburnrate
uv sync
```

---

## Quick start

```bash
# Estimate a query (no connection needed)
uv run dburnrate estimate "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"

# Estimate a file
uv run dburnrate estimate ./queries/daily_revenue.sql

# Estimate a notebook
uv run dburnrate estimate ./notebooks/analysis.ipynb

# With Databricks connection (EXPLAIN + history lookup)
export DBURNRATE_WORKSPACE_URL=https://adb-xxx.azuredatabricks.net
export DBURNRATE_TOKEN=dapi...
uv run dburnrate estimate "SELECT ..." --warehouse-id sql-warehouse-abc
```

Example output:
```
        Cost Estimate
┌─────────────────────┬─────────────────────────────┐
│ Metric              │ Value                       │
├─────────────────────┼─────────────────────────────┤
│ Estimated DBU       │ 0.0312                      │
│ Estimated Cost      │ $0.0062                     │
│ Confidence          │ high                        │
│ Signal              │ historical (14 executions)  │
└─────────────────────┴─────────────────────────────┘
```

---

## What-if scenarios

```bash
# How much cheaper would Photon be?
uv run dburnrate whatif "SELECT ..." --scenario photon

# Serverless SQL migration impact
uv run dburnrate whatif "SELECT ..." --scenario serverless --utilization 60
```

---

## Anti-pattern detection

dburnrate warns you about expensive patterns automatically:

```
⚠  cross_join       CROSS JOIN creates O(n×m) rows — use INNER JOIN with ON clause
⚠  order_by_no_limit  ORDER BY without LIMIT forces a global sort
✗  collect_without_limit  collect() without limit() can OOM the driver
```

---

## Architecture

```
src/dburnrate/
├── core/           models, config, pricing, exchange rates, exceptions
├── parsers/        SQL (sqlglot), PySpark (AST), EXPLAIN COST, Delta log, notebooks
├── estimators/     static (complexity→DBU), hybrid (EXPLAIN+history blend), what-if
├── tables/         Databricks REST client, billing, query history, compute
└── cli/            typer + rich CLI
```

**Key design decisions:**

- `EXPLAIN COST` over execution — strongest cold-start signal, zero data scanned
- `_delta_log` parsing — exact table sizes without touching data
- SHA-256 SQL fingerprinting — recurring query matching against `system.query.history`
- `Decimal` for all money — no float rounding errors
- Per-instance `lru_cache` on exchange rate client — no memory leaks

---

## Programmatic API

```python
from dburnrate.core.models import ClusterConfig
from dburnrate.estimators.static import CostEstimator

cluster = ClusterConfig(instance_type="Standard_DS4_v2", num_workers=4, dbu_per_hour=1.5)
result = CostEstimator(cluster=cluster).estimate("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
print(f"${result.estimated_cost_usd:.4f} ({result.confidence})")
```

See [`docs/usage.md`](docs/usage.md) for the full API including hybrid estimation.

---

## Development

```bash
uv run pytest -m unit -v          # 263 tests
uv run ruff check src/ tests/     # lint
uv run ruff format src/ tests/    # format
uv run bandit -c pyproject.toml -r src/  # security
uv run dburnrate --help
```

---

## Roadmap

| Phase | Status | What |
|-------|--------|------|
| 1 | ✅ | Tests, lint, docstrings |
| 2 | ✅ | Databricks system table client (billing, query history, compute) |
| 3 | ✅ | EXPLAIN COST parser, Delta log reader, hybrid estimator |
| 4 | 🔄 | Wire hybrid into CLI, AWS/GCP pricing, Delta scan sizes |
| 5 | ⏳ | End-to-end fingerprint lookup, production hardening |

Full roadmap in [`FUTURE_TODOS.md`](FUTURE_TODOS.md).
