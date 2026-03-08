# 05 — Research Backlog

> 11 research items with findings from deep research. Items marked ✅ have concrete data; items marked 🔬 need empirical benchmarking on a live workspace.

---

## R1. Compute Throughput Benchmarks 🔬 (P0 — Blocks All Constants)

**Question:** What is actual Parquet scan, shuffle, and join throughput per instance type?

### Findings From Research

**Parquet scan:** CERN measurements (Luca Canali, 2017) on 20-core Xeon servers: **0.05–0.10 GB/s per core**. Databricks Cache boosts by ~4×. NVMe SSD caching up to 5.7× on some TPC-DS queries.

**Azure instance I/O caps (constrain max throughput):**

| Instance | vCPUs | Uncached Disk MB/s | Network Mbps |
|---|---|---|---|
| DS3_v2 | 4 | 192 | 3,000 |
| DS4_v2 | 8 | 384 | 6,000 |
| D16s_v3 | 16 | 384 | 8,000 |
| E8s_v3 | 8 | 192 | 5,000 |

**Shuffle:** Petabyte Sort benchmark (206 i3.8xlarge): 3 GB/s/node disk I/O, 1.1 GB/s/node network. Ousterhout et al. NSDI 2015 found most Spark jobs are CPU-bound, not I/O-bound; shuffle <25% of total runtime for majority of queries.

**Photon speedups (SIGMOD '22 paper, 8-node i3.2xlarge, TPC-H SF=3000):**

| Operation | Photon Speedup |
|---|---|
| Parquet scan | 1.2–2× |
| Hash join | 3–3.5× |
| Aggregation | 3.5–5.7× |
| Sort | ~2× |
| Expression eval | 3–4× |
| Parquet writes | 2× |
| TPC-H overall | 4× avg, 23× max |

**Critical:** Photon DBU rates are ~1.5–2× higher, so workload must achieve sufficient speedup for net savings. Zipher benchmark: join query 1.8× faster but 4× more expensive ($0.07→$0.30).

### SQL Warehouse DBU Rates (Cross-Referenced)

| Size | DBU/hr | Confidence |
|---|---|---|
| 2X-Small | 4 | Confirmed |
| X-Small | ~8 | Inferred |
| Small | 12 | Confirmed |
| Medium | 24 | Confirmed |
| Large | ~48 | Inferred |
| X-Large | ~96 | Inferred |
| 2X-Large | ~192 | Inferred |
| 4X-Large | 528 | Confirmed |

### Still Needed (Empirical)
- Run TPC-DS at 1GB/10GB/100GB on DS3_v2, DS4_v2, D16s_v3 via `databricks/spark-sql-perf`
- Record actual DBU per query — this data does not exist publicly
- Output: calibration JSON file replacing hardcoded constants

---

## R2. EXPLAIN COST Accuracy 🔬 (P1)

**Question:** How accurate are EXPLAIN statistics vs actual execution?

**Method:** Collect EXPLAIN COST for 100+ production queries, execute them, compare `explain_sizeInBytes` vs `actual_read_bytes` from `system.query.history`. Stratify by stats completeness.

**Hypothesis:** With Predictive Optimization (auto-ANALYZE) → ~1.5× error. Without → 10–1000×.

**Key finding from docs:** DBR 16.0+ EXPLAIN output includes stats completeness: `missing`/`partial`/`full`.

---

## R3. AQE Plan Divergence 🔬 (P2)

**Question:** How often does Adaptive Query Execution override the EXPLAIN plan?

**Method:** Compare EXPLAIN COST static plan vs actual physical plan from execution. Track SortMergeJoin→BroadcastHashJoin conversions.

---

## R4. Fingerprint Recurrence 🔬 (P1)

**Question:** What % of queries in a typical workspace have historical fingerprint matches?

**Hypothesis:** Enterprise scheduled jobs: 70–85% recurrence. Ad-hoc analytics: 20–40%.

---

## R5. Billing Attribution Accuracy 🔬 (P0)

**Question:** How accurately can per-query cost be attributed from hourly billing?

**Method:** Single query on dedicated warehouse → compare attributed cost vs actual billing. Repeat at 2, 5, 10 concurrent queries.

**Key decision:** At what concurrency does proportional attribution break? If >5 concurrent, use `total_task_duration_ms` (CPU time) instead of wall-clock duration.

---

## R6. Predictive Optimization Coverage ✅ (P2)

**Findings:**
- PO operates **exclusively on Unity Catalog managed tables** — external/foreign tables excluded
- Uses **stats-on-write** (inline during Photon loading, 7-10× more performant than ANALYZE) + **background refresh** (workload-driven, no fixed schedule)
- Collects both data-skipping stats (per-file min/max/nullCount) AND query optimizer stats (row_count, distinct_count, histograms)
- Selects which columns need statistics based on observed query patterns
- Databricks reports **22% average faster queries** with automatic statistics
- CBO enabled by default; PO stats directly inform cardinality estimation

**Freshness query:**
```sql
SELECT catalog_name, schema_name, table_name,
       MAX(end_time) AS last_analyze_time,
       DATEDIFF(day, MAX(end_time), CURRENT_TIMESTAMP()) AS days_since_analyze
FROM system.storage.predictive_optimization_operations_history
WHERE operation_type = 'ANALYZE' AND operation_status = 'SUCCESSFUL'
GROUP BY 1, 2, 3
ORDER BY days_since_analyze DESC;
```

PO billed under serverless jobs SKU with `billing_origin_product = 'PREDICTIVE_OPTIMIZATION'`.

---

## R7. system.query.history Coverage ✅ (P1)

**Findings — Coverage Map:**

| Query Source | Captured? |
|---|---|
| SQL Warehouses (all types) | ✅ Yes |
| Serverless notebooks/jobs | ✅ Yes |
| JDBC/ODBC to SQL warehouses | ✅ Yes (`client_driver` column) |
| All-purpose (interactive) clusters | ❌ No |
| Jobs clusters (classic) | ❌ No (unless serverless) |
| DLT / Lakeflow pipelines | ❌ No (use `system.lakeflow.*`) |
| Structured Streaming | ❌ No |
| PySpark DataFrame on clusters | ❌ No |

**Schema:** Stores `statement_text` but NOT physical/logical plans. Rich metrics: `total_duration_ms`, `execution_duration_ms`, `read_bytes`, `read_rows`, `produced_rows`, `spilled_local_bytes`, `shuffle_read_bytes`, `written_bytes`, `written_rows`, `written_files`.

**`query_tags`** column (MAP type) supports custom grouping. Set via `SET QUERY_TAGS`, JDBC, Statement API, dbt profiles.

**Columns added July 2025:** `cache_origin_statement_id`, `query_parameters`, `written_rows`, `written_files`.

**Retention:** 365 days, no documented row limits. No streaming reads.

---

## R8. Serverless Billing Granularity ✅ (P2)

**Findings:**
- **No per-query DBU attribution exists.** Serverless SQL Warehouses bill warehouse uptime × per-second DBU rate
- Community confirmed: "DBUs charged based on uptime of compute... running more queries will not increase DBUs unless scaling enabled"
- **January 2026:** Clock-hour-aligned slicing for `job_run_timeline` improves billing joins
- **Serverless jobs:** `job_id`, `job_run_id`, `job_name` well-populated in billing table — per-run cost tracking works
- **Serverless notebooks:** `notebook_id` and `notebook_path` populated (session-level, not per-cell)
- **Serverless SQL warehouses:** February 2026 community reports confirm "almost every field in usage_metadata and identity_metadata are null" — only `warehouse_id` reliable
- **IWM:** Predictions not exposed via any API, system table, or query history column

---

## R9. Cloud VM Pricing APIs ✅ (P0)

**Azure — Zero Auth Required:**
```
https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines' and armRegionName eq 'eastus' and armSkuName eq 'Standard_DS4_v2' and priceType eq 'Consumption'
```
Returns `retailPrice`, `unitPrice`, `armSkuName`, `meterName`. Filter values case-sensitive. Paginated (100/page, follow `NextPageLink`). Python: just `requests`, no SDK.

**AWS:** `boto3` pricing client `get_products()` with `TERM_MATCH` filters. Bulk JSON at `pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/...` is ~3+ GB. `lyft/awspricing` library wraps with caching.

**GCP:** Cloud Billing Catalog API returns vCPU/RAM rates separately. Must calculate: `(vCPU_count × vCPU_rate) + (RAM_GiB × RAM_rate)`.

**Multi-cloud shortcut:** Infracost Cloud Pricing API (`pricing.api.infracost.io/graphql`) — 3M+ prices, all clouds, self-hostable.

**Actual VM Prices (East US, On-Demand):**

| Instance | $/hr | Spot $/hr | Discount |
|---|---|---|---|
| DS3_v2 | $0.293 | $0.054 | 82% |
| DS4_v2 | $0.585 | $0.108 | 82% |
| D16s_v3 | $0.768 | $0.142 | 82% |
| E8s_v3 | $0.504 | $0.096 | 81% |
| m5.xlarge (AWS) | $0.192 | — | 50-70% |
| m5.2xlarge (AWS) | $0.384 | — | 50-70% |
| n2-standard-4 (GCP) | $0.194 | $0.064 | 67% |

---

## R10. Data Layout Impact 🔬 (P3)

**Question:** How much does Liquid Clustering reduce scan costs vs partitioning + Z-ORDER?

**Method:** 100GB Delta table, with/without Liquid Clustering, 20 diverse queries, measure `read_bytes` and `read_files`.

---

## R11. Feature Importance for ML Model 🔬 (P3)

**Question:** Which features explain most variance in query cost?

**Method:** 1,000+ queries with EXPLAIN features + Delta metadata + cluster config + actual cost. Train Random Forest, compute SHAP values.

**Hypothesis:** `read_bytes` dominates, then `shuffle_count`, then `join_type`. Static complexity score <5%.

---

## Lakeflow Job Cost Attribution ✅ (New)

**Canonical SQL pattern** (from Databricks tmm repo, linked from official docs):

```sql
-- Per-job-run cost
SELECT t1.workspace_id, t2.name, t1.job_id, t1.run_id,
       SUM(list_cost) as list_cost
FROM (
  SELECT workspace_id, usage_metadata.job_id,
         usage_metadata.job_run_id as run_id,
         SUM(usage_quantity * list_prices.pricing.default) as list_cost
  FROM system.billing.usage t1
  INNER JOIN system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  WHERE billing_origin_product = 'JOBS'
    AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY ALL
) t1
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
  FROM system.lakeflow.jobs QUALIFY rn=1
) t2 USING (workspace_id, job_id)
GROUP BY ALL ORDER BY list_cost DESC
```

**Timeline join pattern:**
```sql
INNER JOIN system.lakeflow.job_run_timeline t2
  ON t1.workspace_id = t2.workspace_id
  AND t1.usage_metadata.job_id = t2.job_id
  AND t1.usage_metadata.job_run_id = t2.run_id
  AND t1.usage_start_time >= date_trunc("Hour", t2.period_start_time)
  AND t1.usage_start_time < date_trunc("Hour", t2.period_end_time) + INTERVAL 1 HOUR
```

**All-purpose cluster gap:** `usage_metadata.job_id` is NULL for jobs on all-purpose clusters. Databricks official: "Precise cost calculation for jobs on [all-purpose] compute is not possible with 100% accuracy."
