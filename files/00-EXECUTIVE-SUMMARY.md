# dburnrate — Comprehensive Project Audit

> Independent review covering code quality, architecture, features, research gaps, enterprise readiness, testing, agent workflows, and performance optimization.
> Conducted March 2026. Read alongside DESIGN.md.

---

## Audit Scope

This audit examined every source file in `src/dburnrate/`, all 263 tests, DESIGN.md (906 lines), AGENTS.md, all task files, and the project knowledge base. It cross-references findings against Databricks documentation, academic literature, and the pricing/billing APIs as of early 2026.

## Documents in This Audit

| Document | Covers |
|---|---|
| [01-CRITICAL-CODE-FIXES.md](01-CRITICAL-CODE-FIXES.md) | Mathematical errors, fabricated constants, SQL injection, broken formulas |
| [02-ARCHITECTURE-GAPS.md](02-ARCHITECTURE-GAPS.md) | Missing orchestrator, no billing attribution, dual-mode runtime, pricing model |
| [03-ENTERPRISE-SUPPORT.md](03-ENTERPRISE-SUPPORT.md) | System table view registry, governance patterns, column mapping |
| [04-FEATURE-ROADMAP.md](04-FEATURE-ROADMAP.md) | 12 new features: dual-bill TCO, compute advisor, notebook aggregation, cost regression, waste detection, etc. |
| [05-RESEARCH-BACKLOG.md](05-RESEARCH-BACKLOG.md) | 11 research items with findings from deep research on throughput, pricing APIs, billing granularity, query.history coverage, Predictive Optimization, Lakeflow attribution |
| [06-TESTING-STRATEGY.md](06-TESTING-STRATEGY.md) | Testing deficiencies, accuracy benchmarks, property-based testing, integration infrastructure |
| [07-AGENT-WORKFLOW.md](07-AGENT-WORKFLOW.md) | Planner/executor validation loop, task specification gaps, DESIGN.md conflicts |
| [08-PERFORMANCE-RUST.md](08-PERFORMANCE-RUST.md) | When Rust makes sense, PyO3 architecture, alternatives (sqlglotrs, server-side fingerprinting) |

---

## Top 10 Findings (Priority Order)

### 1. The Static Estimator Formula Is Mathematically Wrong
**File:** `estimators/static.py` — Formula is `complexity² × cluster_factor / 100`, producing 960× overestimates for simple queries. A GROUP BY on 2 workers estimates 0.96 DBU; reality is ~0.001 DBU. Must be replaced with a linear model.

### 2. No Databricks Runtime Support
The package is built entirely as an external CLI. Zero awareness of `SparkSession`, `DATABRICKS_RUNTIME_VERSION`, or `dbutils`. For 70% in-cluster use, every system table query takes a wasteful REST round-trip through the Statement Execution API back into the same cluster. Needs a `RuntimeBackend` abstraction with `SparkBackend` and `RestBackend` implementations.

### 3. Hardcoded System Table Paths Break Enterprise Governance
Eight hardcoded `system.billing.usage`, `system.query.history`, etc. references across 3 files. Enterprise environments where system tables are hidden behind curated views cannot use dburnrate. Needs a configurable `TableRegistry`.

### 4. Hybrid Estimator Uses Fictional Pricing ($0.20/DBU)
`_NOMINAL_USD_PER_DBU = 0.20` matches no real Databricks SKU. ALL_PURPOSE is $0.55, Jobs is $0.30, SQL Serverless is $0.70. Every hybrid estimate is systematically wrong. Must use `get_dbu_rate(sku)` from the pricing module.

### 5. EXPLAIN COST and Fingerprinting Are Built But Disconnected
The CLI (`cli/main.py`) only uses the static estimator. `HybridEstimator`, `DatabricksClient`, fingerprinting, Delta metadata, and EXPLAIN parsing all exist but are not wired into the user-facing surface. Phase 4 tasks describe this but no orchestration module exists.

### 6. No Calibration Methodology or Benchmark Dataset
263 tests verify types and edge cases. Zero tests verify estimate accuracy. No benchmark queries with known actual costs exist. Every constant in the estimator (`_SCAN_DBU_PER_GB = 0.5`, `_SHUFFLE_DBU_EACH = 0.2`, join weights) is fabricated with no empirical grounding.

### 7. DBU-Only Estimates Miss 40-60% of Classic Compute Cost
For classic clusters (Jobs, All-Purpose, DLT), the Azure VM infrastructure is a separate bill. A DS4_v2 cluster costs $0.585/hr in VM fees on top of DBU fees. dburnrate shows only the DBU half. Needs total cost of ownership with VM pricing integration.

### 8. Missing Cost Attribution Module
DESIGN.md references `tables/attribution.py` for billing↔query history correlation. This file doesn't exist. Without it, there's no ground truth for calibrating estimates, no "last time this cost $X" signal, and no training data for the Phase 6 ML model.

### 9. The Project Tries to Do Too Much
13+ phases spanning static analysis, EXPLAIN, Delta, fingerprinting, ML, Prophet, DLT, model serving, batch, self-referential estimation, and CI/CD. No commercial tool has solved pre-execution estimation for Databricks. Recommendation: narrow to "SQL warehouse cost estimation accurate to 3×" as v0.1, defer everything else.

### 10. Anti-Pattern Detector Uses String Matching
`antipatterns.py` detects `CROSS JOIN` via `"CROSS JOIN" in sql.upper()`, which matches inside string literals, comments, and identifiers. Should use sqlglot AST traversal (which already exists in `sql.py`).

---

## Revised Phase Plan

| Phase | What | Status | Effort |
|---|---|---|---|
| **4A** | Wire hybrid pipeline into CLI + fix pricing constants | 🔴 Blocked by architecture | 1-2 weeks |
| **4B** | Calibration research: throughput benchmarks + billing attribution | 🔴 No data exists | 2-3 weeks |
| **4C** | Enterprise support: TableRegistry + RuntimeBackend | 🔴 Not started | 1 week |
| **4D** | Ship `dburnrate lint` as standalone feature | 🟢 Ready today | 2 days |
| **5** | Production hardening: caching, error handling, observability | ⏳ Planned | 1-2 weeks |
| **6** | ML cost bucket classification (requires 4B data) | ⏳ Planned | 3-4 weeks |
| **v0.2** | Notebook aggregation, compute advisor, cost regression detection | ⏳ Deferred | 4-6 weeks |
| **v0.3** | CI/CD gates, DABs integration, forecasting | ⏳ Deferred | 6-8 weeks |

### Dropped from Roadmap
- **Phase 11** (self-referential `estimate_self()`): Gimmick, no real user value
- **Phases 7-10**: Merge into Phase 5, eliminate duplicate scope with existing phases
