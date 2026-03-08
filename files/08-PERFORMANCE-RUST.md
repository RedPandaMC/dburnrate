# 08 — Performance Optimization & Rust

> When and where Rust acceleration makes sense for dburnrate.

---

## 8.1 Current Execution Profile

For `dburnrate estimate` in connected mode:

| Component | % Wall Time | Bottleneck |
|---|---|---|
| Network I/O (REST calls) | 85-95% | Waiting on Databricks |
| SQL parsing (sqlglot) | ~10-50ms | CPU, already optimized |
| EXPLAIN regex parsing | ~1-5ms | CPU, negligible |
| SQL fingerprinting | ~0.1-1ms | CPU, negligible |
| Cost arithmetic (Decimal) | ~μs | Not measurable |

**Verdict:** ~95% network wait, ~4% Python parsing, ~1% everything else. Rewriting the 1% in Rust gives a faster 1%.

---

## 8.2 When Rust Becomes Justified

### Scenario 1: Batch Analysis (Phase 12)

`dburnrate estimate-batch "queries/**/*.sql"` processing 5,000 SQL files offline (100% CPU, zero network):

- At 30ms/query Python: **150 seconds**
- At 1ms/query Rust: **5 seconds**
- Hot loop: parse SQL → extract operations → compute complexity → fingerprint → score

### Scenario 2: Large-Scale Fingerprint Matching

`find_similar_queries()` fetches up to `limit × 100` rows and fingerprints client-side. At 10K queries/day × 30 days = 300K SQL strings:

- `normalize_sql()` at 0.5ms each Python: **150 seconds**
- Rust with rayon parallelism: **2-3 seconds**

---

## 8.3 PyO3 Architecture (If Needed)

Single extension module, optional dependency:

```
src/dburnrate/
├── _native/              # Rust extension (optional)
│   ├── Cargo.toml
│   └── src/lib.rs        # PyO3 bindings
├── parsers/
│   ├── sql.py            # Falls back to sqlglot if _native unavailable
```

```rust
use pyo3::prelude::*;
use sha2::{Sha256, Digest};

#[pyfunction]
fn normalize_and_fingerprint(sql: &str) -> String {
    let normalized = normalize(sql);  // Combined normalize + hash
    hex::encode(Sha256::digest(normalized.as_bytes()))
}

#[pyfunction]
fn fingerprint_batch(queries: Vec<&str>) -> Vec<String> {
    queries.par_iter()  // rayon parallelism
        .map(|q| normalize_and_fingerprint(q))
        .collect()
}

#[pyfunction]
fn parse_explain_plan(text: &str) -> PyResult<ExplainPlan> {
    // All 6 regex patterns in one pass
}
```

Python graceful fallback:
```python
try:
    from dburnrate._native import normalize_and_fingerprint, fingerprint_batch
except ImportError:
    from dburnrate.parsers._fingerprint import normalize_and_fingerprint
    def fingerprint_batch(queries):
        return [normalize_and_fingerprint(q) for q in queries]
```

Build: `maturin` backend, `pip install dburnrate[native]` for Rust acceleration.

---

## 8.4 Better Alternatives Before Rust

### 1. Push Fingerprinting to Databricks SQL
Instead of fetching 300K rows and fingerprinting client-side:
```sql
SELECT SHA2(UPPER(REGEXP_REPLACE(statement_text, '\\d+', '?')), 256) as fingerprint, ...
FROM system.query.history
WHERE ...
```
Eliminates client-side bottleneck entirely.

### 2. Use `sqlglotrs` for Tokenization
sqlglot already has an optional Rust tokenizer. Adding `sqlglotrs` as an optional dependency gives 2-5× faster parsing with zero custom Rust code.

### 3. Cache Aggressively
TTL cache on `normalize_and_fingerprint` prevents re-hashing the same query text. `lru_cache` on `DESCRIBE DETAIL` results eliminates repeated network calls.

---

## 8.5 Recommendation

| Phase | Strategy |
|---|---|
| **v0.1** | Pure Python. Server-side fingerprinting. Cache aggressively. |
| **v0.2** | Add `sqlglotrs` optional dependency for batch mode. Profile. |
| **v0.3+** | If profiling confirms parsing bottleneck in batch: add `_native` module with `maturin`. |

The engineering cost of Rust (CI for manylinux wheels, macOS universal2, Windows MSVC, Rust toolchain requirement for contributors) is not justified until batch mode ships and profiling confirms parsing dominates.
