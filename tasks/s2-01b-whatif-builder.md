# Task: Fluent WhatIfBuilder with Method Chaining (Data Source Focus)

---

## Metadata

```yaml
id: s2-01b-whatif-builder
status: todo
sprint: 2
priority: high
agent: ~
blocked_by: [s1-02-instance-catalog]
created_by: planner
```

---

## Context

### Goal

Rewrite `estimators/whatif.py` from standalone functions into a fluent `WhatIfBuilder` class with method chaining. This is the "cuts like butter" UX — users chain modifications naturally and get a before/after comparison.

This task expands the original `legacy.py` pattern to include:
- **Data source layer** scenarios (Delta, Liquid Clustering, caching, partitioning)
- **Spark config** scenarios (shuffle partitions, broadcast threshold, AQE)
- **Cost transparency** showing verified vs estimated multipliers
- **Comparison groups** for side-by-side scenario analysis

### Files to Read

```
src/burnt/estimators/whatif.py    # Current standalone functions (to be rewritten)
src/burnt/core/models.py          # CostEstimate, ClusterConfig
src/burnt/core/instances.py       # InstanceSpec, AzureInstanceCatalog (from s1-02)
DESIGN.md § "Fluent What-If Builder"  # Target API
tasks/s2-01b-whatif-builder.md    # This specification
```

### Files to Rewrite

```
src/burnt/estimators/whatif.py     # Full rewrite: WhatIfBuilder class + builders
tests/unit/test_whatif.py          # Tests for builder pattern
```

### Files to Modify

```
src/burnt/__init__.py              # Export what_if() convenience function
src/burnt/core/models.py           # Add WhatIfResult, WhatIfModification models
```

---

## Specification

### API Design: Context-Based Builders

Users access different configuration categories through explicit context methods for clarity:

```python
# Single scenario
result = (estimate.what_if()
    .cluster()              # Cluster configuration context
        .to_instance("Standard_DS3_v2")
        .enable_photon()
        .use_spot()
    .data_source()          # Data source configuration context
        .to_delta_format()
        .enable_liquid_clustering(keys=["date", "customer_id"])
        .enable_disk_cache()
    .spark_config()         # Spark configuration context
        .with_shuffle_partitions(200)
        .with_aqe_enabled()
    .compare())

# Multiple scenarios with top-level modifications applied to all
result = (estimate.what_if()
    .cluster().enable_photon()  # Applied to ALL scenarios including Baseline
    .scenarios([
        ("Downsize", lambda b: b.cluster().to_instance("DS3_v2")),  # Photon + downsize
        ("Full", lambda b: (
            b.cluster().to_instance("DS3_v2")
            .data_source().to_delta_format()
        )),  # Photon + downsize + Delta
    ])
    .compare())
```

### WhatIfBuilder API

```python
class WhatIfBuilder:
    """Fluent builder for what-if cost scenario modeling."""
    
    def __init__(self, estimate: CostEstimate, cluster: ClusterConfig): ...
    
    # Context methods for category separation
    def cluster(self) -> ClusterBuilder: ...
    def data_source(self) -> DataSourceBuilder: ...
    def spark_config(self) -> SparkConfigBuilder: ...
    
    # Define multiple scenarios to compare
    # - Baseline scenario is always added automatically
    # - Top-level modifications (before .scenarios()) apply to all scenarios
    # - Scenario-specific modifications can override/opt-out
    def scenarios(self, scenarios: list[tuple[str | None, Callable]]) -> "WhatIfBuilder": ...
    
    # Discovery: prints available options directly
    def options(self) -> None: ...
    
    # Compare scenarios
    # - If only one scenario: returns WhatIfResult
    # - If multiple scenarios (via .scenarios()): returns MultiScenarioResult
    def compare(self) -> WhatIfResult | MultiScenarioResult: ...
```

### Cluster Builder

```python
class ClusterBuilder:
    """Builder for cluster configuration changes."""
    
    def __init__(self, parent: WhatIfBuilder): ...
    
    def to_instance(self, sku: str) -> "ClusterBuilder": ...
    def enable_photon(self) -> "ClusterBuilder": ...
    def use_spot(self, fallback: bool = True) -> "ClusterBuilder": ...
    def set_workers(self, count: int) -> "ClusterBuilder": ...
    def to_serverless(self) -> "ClusterBuilder": ...
    
    # Return to parent builder
    def data_source(self) -> DataSourceBuilder: ...
    def spark_config(self) -> SparkConfigBuilder: ...
    def compare(self) -> WhatIfResult: ...
```

### DataSource Builder

```python
class DataSourceBuilder:
    """Builder for data source optimization changes."""
    
    def __init__(self, parent: WhatIfBuilder): ...
    
    # Format migration
    def to_delta_format(self) -> "DataSourceBuilder": ...
    
    # Clustering (Delta only - automatically applies Delta format)
    def enable_liquid_clustering(self, keys: list[str]) -> "DataSourceBuilder": ...
    def set_partitioning(self, column: str) -> "DataSourceBuilder": ...
    
    # Caching
    def enable_disk_cache(self) -> "DataSourceBuilder": ...
    
    # File operations
    def compact_files(self, target_mb: int = 128) -> "DataSourceBuilder": ...
    
    # Optimization hints (printed inline during display)
    def enable_column_pruning(self) -> "DataSourceBuilder": ...
    def enable_file_skipping(self) -> "DataSourceBuilder": ...
    
    def set_compression(self, codec: str = "zstd") -> "DataSourceBuilder": ...
    
    # Return to parent builder
    def cluster(self) -> ClusterBuilder: ...
    def spark_config(self) -> SparkConfigBuilder: ...
    def compare(self) -> WhatIfResult: ...
```

### SparkConfig Builder

```python
class SparkConfigBuilder:
    """Builder for Spark configuration changes."""
    
    def __init__(self, parent: WhatIfBuilder): ...
    
    # Shuffle optimization
    def with_shuffle_partitions(self, count: int) -> "SparkConfigBuilder": ...
    def with_auto_shuffle_partitions(self) -> "SparkConfigBuilder": ...
    
    # Broadcast optimization
    def with_broadcast_threshold_mb(self, mb: int) -> "SparkConfigBuilder": ...
    
    # AQE (Adaptive Query Execution)
    def with_aqe_enabled(self, coalesce: bool = True) -> "SparkConfigBuilder": ...
    
    # Generic setter for edge cases
    def set(self, key: str, value: str | int | bool) -> "SparkConfigBuilder": ...
    
    # Return to parent builder
    def cluster(self) -> ClusterBuilder: ...
    def data_source(self) -> DataSourceBuilder: ...
    def compare(self) -> WhatIfResult: ...
```

### Aliases (3-Letter Shortcuts)

```python
# Import from burnt.whatif.aliases (explicit import required)
from burnt.whatif.aliases import clsr, data, conf

# Use in Python API
result = estimate.what_if().clsr().enable_photon().compare()  # .clsr() = .cluster()
result = estimate.what_if().data().to_delta_format().compare()  # .data() = .data_source()
result = estimate.what_if().conf().with_aqe_enabled().compare()  # .conf() = .spark_config()
```

**Alias Mapping:**
- `clsr` = `cluster()`
- `data` = `data_source()`
- `conf` = `spark_config()`

**CLI Flags (default import, no explicit import needed):**
```bash
# Short flags for scenario types
burnt what-if "SELECT ..." --clsr --photon                # --clsr = --cluster
burnt what-if "SELECT ..." --data --delta                 # --data = --data-source
burnt what-if "SELECT ..." --conf --shuffle-partitions 200 # --conf = --spark-config

# Multiple scenarios (baseline auto-added as Scenario 1)
burnt what-if "SELECT ..." \
  --scenario --clsr --photon \
  --scenario --clsr --instance DS3_v2

# Top-level modifications apply to all scenarios
burnt what-if "SELECT ..." --clsr --photon \
  --scenario --instance DS3_v2 \
  --scenario --data --delta
```

### WhatIfResult Models

```python
class WhatIfModification(BaseModel):
    name: str                   # e.g., "Enable Photon", "Migrate to Delta"
    cost_multiplier: float      # e.g., 0.66 = 34% cheaper
    is_verified: bool           # True if from official benchmarks
    rationale: str              # Human-readable explanation
    trade_offs: list[str]       # e.g., ["Requires 2× speedup to break even"]

class WhatIfResult(BaseModel):
    original: CostEstimate
    projected: CostEstimate
    modifications: list[WhatIfModification]
    total_savings_pct: float
    recommended_cluster: ClusterConfig | None
    
    # Cost transparency methods
    def get_verified_multipliers(self) -> list[str]: ...
    def get_estimated_multipliers(self) -> list[str]: ...
    
    def display(self) -> None: ...  # Prints inline with cost transparency
    def comparison_table(self) -> str: ...
    def summary(self) -> str: ...

class MultiScenarioResult(BaseModel):
    """Result of comparing multiple what-if scenarios."""
    scenarios: list[tuple[str, WhatIfResult]]  # Includes auto-added Baseline
    
    def display(self) -> None: ...  # Prints comparison table directly
    def get_results(self) -> list[WhatIfResult]: ...
```

### Entry Points

```python
# Single scenario - method chaining
result = estimate.what_if().cluster().enable_photon().compare()
result.display()

# Single scenario - function pattern
result = burnt.compare(estimate.what_if().cluster().enable_photon())
result.display()

# From raw parameters (convenience function at package level)
result = burnt.what_if(dbu=10.0, sku="ALL_PURPOSE").cluster().to_serverless().compare()
result.display()

# Multiple scenarios - top-level mods apply to all, baseline auto-added
result = (estimate.what_if()
    .cluster().enable_photon()  # Applied to ALL scenarios including Baseline
    .scenarios([
        ("Downsize", lambda b: b.cluster().to_instance("DS3_v2")),  # Photon + downsize
        ("Full", lambda b: (
            b.cluster().to_instance("DS3_v2")
            .data_source().to_delta_format()
        )),  # Photon + downsize + Delta
    ])
    .compare())
result.display()
# Result: 3 scenarios (Baseline with photon, Downsize with photon+downsize, Full with all)

# Multiple scenarios - function pattern
result = burnt.compare(
    estimate.what_if()
    .cluster().enable_photon()
    .scenarios([
        ("Downsize", lambda b: b.cluster().to_instance("DS3_v2")),
        ("Full", lambda b: b.cluster().to_instance("DS3_v2").data_source().to_delta_format()),
    ])
)
result.display()
```

### Cost Calculation Rules

**Data Source Layer Multipliers:**
- **Delta vs Parquet format:** 0.6-0.8x (30-40% savings) *Estimated*
- **Liquid Clustering:** 0.3-0.5x (50-70% savings for qualifying queries) *Estimated*
- **Disk Cache (high hit rate):** 0.1-0.2x (80-90% savings) *Estimated*
- **Column Pruning/Predicate Pushdown:** 0.5-0.7x *Estimated*

**Cluster Multipliers:**
- **SKU migration:** Apply rate ratio (ALL_PURPOSE $0.55 → JOBS $0.30 = 0.545× cost)
- **Photon:** Apply `2.5× DBU multiplier / speedup_factor` per operation type
- **Spot:** Apply 60-80% VM cost reduction (DBU unchanged)

**Spark Config Multipliers:**
- **AQE enabled:** 0.8-0.9x (10-20% savings) *Verified by Databricks*
- **Optimized shuffle partitions:** 0.9-0.95x *Estimated*
- **Broadcast join optimization:** 0.6-0.8x for qualifying joins *Estimated*

**Stacking:** Modifications stack multiplicatively

**Transparency:** Each modification shows `is_verified` flag with source reference

**Scenario Rules:**
- **Baseline:** Always added automatically as the first scenario
- **Top-level modifications:** Applied to all scenarios unless overridden
- **Opting out:** Remove top-level mod and re-specify in scenario to override

---

## Acceptance Criteria

- [ ] `WhatIfBuilder` with `cluster()`, `data_source()`, `spark_config()` context methods
- [ ] Each context builder returns `self` for chaining
- [ ] `.scenarios([...])` method for multi-scenario comparison
- [ ] Top-level modifications apply to all scenarios; can be overridden in scenario
- [ ] Baseline scenario auto-added by default (no typing required)
- [ ] `.options()` prints available options directly to console
- [ ] `.compare()` returns `WhatIfResult` for single scenario, `MultiScenarioResult` for multiple
- [ ] `burnt.compare(scenario)` function works as alternative to `.compare()` method
- [ ] `WhatIfResult.display()` shows verified vs estimated multipliers inline
- [ ] `MultiScenarioResult.display()` prints comparison table directly (side-by-side)
- [ ] `CostEstimate.what_if()` returns a `WhatIfBuilder` bound to the estimate
- [ ] `burnt.what_if(dbu=, sku=)` convenience function at package level
- [ ] `.enable_liquid_clustering()` automatically applies Delta format with warning
- [ ] Modifications stack correctly (multiplicative)
- [ ] Photon warns when speedup insufficient for break-even
- [ ] Existing `apply_photon_scenario`, `apply_cluster_resize` preserved as internal helpers
- [ ] **Aliases**: `from burnt.whatif.aliases import clsr, data, conf` works (3-letter aliases)
- [ ] **CLI Flags**: `--clsr`, `--data`, `--conf` work as shortcuts for `--cluster`, `--data-source`, `--spark-config`
- [ ] **CLI Scenarios**: `--scenario` flag with optional name, auto-generated if not provided
- [ ] **CLI Output**: Side-by-side comparison table for multi-scenario
- [ ] Unit tests: chaining, each builder, stacking, display output, multi-scenario comparison, aliases
- [ ] **Remove**: Join strategy methods (too complex for this sprint)
- [ ] **Remove**: Presets (users want explicit control)
- [ ] **Remove**: Direct cluster methods on main builder (force context usage)

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
