# Benchmark Dataset for burnt

This directory contains reference queries, expected cost ranges, and accuracy validation tests for the burnt cost estimation system.

## Overview

The benchmark dataset provides:
- **Reference SQL queries** with varying complexity
- **Expected cost ranges** derived from real Databricks billing data
- **Monotonicity tests** to ensure estimation sanity
- **Property-based tests** using Hypothesis
- **Integration fixtures** for testing with/without Databricks

## Data Sources

### Real Billing Data (system_tables_masked.xlsx)

The expected cost ranges are derived from a masked dataset containing 250 real billing records:

| Statistic | Value |
|-----------|-------|
| Total Records | 250 |
| DBU Range | 0.025 - 13.5 |
| DBU Mean | 2.16 |
| DBU Median | 0.50 |
| Time Period | 30 days |
| Cloud | AZURE |

### SKU Distribution

| SKU | Percentage | $/DBU |
|-----|------------|-------|
| PREMIUM_JOBS_COMPUTE | 70% | $0.30 |
| PREMIUM_SQL_PRO_COMPUTE_EU_NORTH | 15% | $0.70 |
| PREMIUM_ALL_PURPOSE_COMPUTE | 10% | $0.55 |
| PREMIUM_JOBS_COMPUTE_(PHOTON) | 4% | $0.30 |
| PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH | 1% | $0.70 |

## Queries

| Query | Complexity | Expected DBU |
|-------|-----------|--------------|
| `simple_select.sql` | Trivial | 0.01 - 0.10 |
| `single_table_filter.sql` | Low | 0.02 - 0.20 |
| `groupby_agg.sql` | Medium | 0.05 - 0.50 |
| `two_table_join.sql` | Medium | 0.10 - 1.00 |
| `five_table_join.sql` | High | 0.50 - 5.00 |

## Running Benchmarks

```bash
# Run all benchmark tests
uv run pytest tests/benchmarks/ -v

# Run with benchmark marker
uv run pytest -m benchmark -v

# Run monotonicity tests only
uv run pytest tests/benchmarks/test_monotonicity.py -v

# Run property-based tests
uv run pytest tests/benchmarks/test_property.py -v
```

## Accuracy Targets

| Sprint | Target | Method |
|--------|--------|--------|
| 1-2 | Within **10×** | Static + heuristics |
| 3 | Within **3×** | Full pipeline (Delta + EXPLAIN + history) |
| 5 | Within **2×** | ML models |

## Structure

```
tests/benchmarks/
├── README.md                 # This file
├── conftest.py              # Fixtures and pytest configuration
├── queries/                 # SQL query files
│   ├── simple_select.sql
│   ├── single_table_filter.sql
│   ├── groupby_agg.sql
│   ├── two_table_join.sql
│   └── five_table_join.sql
├── expected_costs.json      # Known cost ranges
├── test_monotonicity.py    # Sanity tests
└── test_property.py        # Hypothesis tests
```

## Adding New Queries

1. Create SQL file in `queries/`
2. Add entry to `expected_costs.json`
3. Add monotonicity test in `test_monotonicity.py`
4. Run tests: `uv run pytest tests/benchmarks/ -v`