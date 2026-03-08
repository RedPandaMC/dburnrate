# 04 — Feature Roadmap

> 12 new features organized by priority, with effort estimates and implementation notes.

---

## F1. Total Cost of Ownership — The Dual-Bill (P0)

**The single largest gap.** Every estimate today is DBU-only. For classic compute, VM infrastructure is 40-60% of the total bill.

Example: A query on 4× DS4_v2 Jobs Compute costs 0.5 DBU ($0.15 DBU fee) + $0.585/hr × 5 nodes × query_seconds/3600 in VM fees. Current output shows only $0.15.

**Implementation:**
- VM pricing table per cloud/region/instance type via Azure Retail Prices API (zero auth), AWS Pricing API, GCP Cloud Billing Catalog
- Total cost: `(dbu × dbu_rate) + (vm_hours × vm_rate × (1_driver + num_workers))` for classic; `dbu × serverless_rate` for serverless
- Default output shows total cost; `--dbu-only` flag for DBU-only view
- **Shortcut:** Infracost Cloud Pricing API (`pricing.api.infracost.io/graphql`) covers all three clouds with a single GraphQL interface

**Effort:** 1 week

---

## F2. Notebook-Level Cost Aggregation (P0)

The core "estimate before Run All" experience. Parse entire notebooks, estimate each cell independently, sum with per-cell breakdown.

```bash
dburnrate estimate ./notebooks/daily_etl.ipynb --breakdown
```

```
  Notebook Cost Estimate: daily_etl.ipynb
┌──────┬──────────┬──────────┬─────────────────────────────┐
│ Cell │ Language │ Est. Cost │ Summary                     │
├──────┼──────────┼──────────┤─────────────────────────────┤
│ 3    │ SQL      │ $0.0021  │ SELECT ... FROM dim_product  │
│ 5    │ SQL      │ $0.4812  │ MERGE INTO fact_sales ...    │  ← 94%
│ 7    │ PySpark  │ $0.0094  │ df.groupBy().agg()           │
│ 9    │ SQL      │ $0.0187  │ INSERT INTO gold.summary     │
├──────┼──────────┼──────────┤─────────────────────────────┤
│ TOTAL│          │ $0.5114  │ 4 cells analyzed             │
└──────┴──────────┴──────────┴─────────────────────────────┘
⚠ Cell 5 accounts for 94% of estimated cost.
```

The notebook parser (`parsers/notebooks.py`) already extracts cells and detects languages. The missing piece is looping, estimating each, and aggregating.

**Effort:** 3 days

---

## F3. `--explain` Transparency Flag (P0)

Show the full estimation pipeline breakdown so users understand and trust estimates.

```bash
dburnrate estimate "SELECT ..." --warehouse-id abc --explain
```

```
  Estimation Pipeline
  ┌─ Tier 3: Historical Match ─────────────────────┐
  │ Fingerprint: a3f8c2d1...                       │
  │ 14 matching executions, p50: 4,200ms           │
  │ Attributed cost (p50): $0.038                  │
  │ Confidence: HIGH                               │
  └────────────────────────────────────────────────┘
  ┌─ Tier 2: EXPLAIN COST ────────────────────────┐
  │ Root sizeInBytes: 2.4 GB | rowCount: 12.8M    │
  │ Join: BroadcastHashJoin | Shuffles: 2         │
  │ Stats: FULL | Estimated cost: $0.041           │
  └────────────────────────────────────────────────┘
  Final: $0.039 (weighted: 60% historical, 30% EXPLAIN, 10% static)
```

**Effort:** 2 days

---

## F4. `dburnrate lint` — Anti-Pattern Detection as Standalone (P0)

Anti-pattern detection works today with zero calibration. Ship it separately while cost estimation accuracy improves.

```bash
dburnrate lint ./queries/
```

```
⚠ daily_revenue.sql:12  ORDER BY without LIMIT forces global sort
✗ etl_pipeline.sql:45   collect() without limit() — will OOM on large tables
⚠ etl_pipeline.sql:67   Python UDF has 10-100x overhead vs Pandas UDF
```

**Effort:** 2 days (refactor existing `antipatterns.py` + add CLI command)

---

## F5. Compute Type Advisor (P1)

Same query priced across compute types — trivial to implement, high user value.

```bash
dburnrate advise "SELECT ..." --current-sku ALL_PURPOSE
```

```
  Compute Migration Analysis
┌──────────────────┬───────────┬──────────┬────────────────────┐
│ Compute Type     │ Est. Cost │ Savings  │ Tradeoff           │
├──────────────────┼───────────┼──────────┤────────────────────┤
│ All-Purpose      │ $0.0412   │ baseline │ Interactive        │
│ Jobs Compute     │ $0.0225   │ -45%     │ No notebook UI     │
│ SQL Serverless   │ $0.0526   │ +28%     │ Fastest cold start │
└──────────────────┴───────────┴──────────┴────────────────────┘
```

**Effort:** 3 days

---

## F6. Hidden Cost Audit (P1)

Surface background serverless consumers most users don't know exist: Predictive Optimization, materialized view refreshes, Lakehouse Monitoring, fine-grained access control.

```bash
dburnrate audit --days 30
```

```
  Hidden Cost Audit (Last 30 Days)
┌───────────────────────────┬──────────┬───────────┐
│ Feature                   │ DBUs     │ Est. Cost │
├───────────────────────────┼──────────┼───────────┤
│ Predictive Optimization   │ 142.3    │ $42.69    │
│ Materialized View Refresh │ 89.7     │ $62.79    │
│ Lakehouse Monitoring      │ 23.1     │ $6.93     │
├───────────────────────────┼──────────┼───────────┤
│ TOTAL background spend    │ 255.1    │ $112.41   │
└───────────────────────────┴──────────┴───────────┘
```

Pure system table query — filter `system.billing.usage` by `billing_origin_product` values. No estimation needed.

**Effort:** 2 days

---

## F7. Cost Regression Detection (P1)

The killer feature for recurring jobs. Detect when the same query template suddenly costs more.

```bash
dburnrate trend --fingerprint "SELECT ..." --days 30
```

Detect: cost spikes (p50 jumped >2×), data growth impact (read_bytes doubled), plan regression (join strategy changed), cluster/SKU change.

**Effort:** 1 week

---

## F8. Idle Cluster & Waste Detection (P2)

Cross-reference `system.compute.node_timeline` with `system.query.history`: clusters running >2 hours with zero queries, All-Purpose used for jobs workloads, oversized clusters, warehouses with long auto-stop.

```bash
dburnrate waste --days 7
```

**Effort:** 1 week

---

## F9. Committed-Use Discount Modeling (P2)

Given 90-day usage, model whether a DBCU commitment saves money.

```bash
dburnrate commitment --days 90
```

Time-series analysis on `system.billing.usage` to project optimal commitment level.

**Effort:** 3 days

---

## F10. Spot Instance Modeling (P2)

For classic Jobs Compute, model spot VM savings vs interruption risk.

**Effort:** 2 days

---

## F11. Lakeflow Job DAG Estimation (P3)

Estimate cost of multi-task job DAGs. Parse job definition JSON, estimate each task, sum with parallelism awareness.

```bash
dburnrate estimate-job --job-id 700809544510906
```

Uses `system.lakeflow.jobs` and `system.lakeflow.job_run_timeline` (GA January 2026) with clock-hour-aligned slicing.

**Effort:** 2 weeks

---

## F12. Data Layout Impact Advisor (P3)

Analyze a table and estimate cost impact of Liquid Clustering vs current layout. Combine Delta log file-level min/max statistics with query predicate analysis from sqlglot.

```bash
dburnrate layout catalog.schema.my_table --query "SELECT ... WHERE region = 'US'"
```

**Effort:** 2 weeks (requires R10 research first)

---

## Future (v0.2+)

| Feature | Description | Prerequisite |
|---|---|---|
| CI/CD Cost Gate | `dburnrate compare baseline.json current.json --threshold 200%` | Calibrated estimates |
| DABs Integration | `dburnrate estimate-bundle ./databricks.yml` | Job DAG estimation |
| Notebook Widget | `dburnrate.install_widget()` in Databricks UI | RuntimeBackend |
| VS Code Extension | Inline `💰 ~$0.003` annotations per SQL line | Language server protocol |
