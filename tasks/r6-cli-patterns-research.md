# Task: Research CLI Patterns and Method Aliases for What-If Builder

---

## Metadata

```yaml
id: r6-cli-patterns-research
status: done
phase: 0
priority: medium
agent: opencode/mimo-v2-flash-free
completed_by: opencode/mimo-v2-flash-free
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Research CLI patterns for multi-scenario comparison, method aliases for fluent builders, and display patterns for `compare()` and `display()` functions. Provide concrete recommendations for the what-if builder tool.

### Files Researched

- AWS CLI, gcloud, kubectl, terraform command patterns
- Python libraries with method aliases (numpy, pandas, requests)
- Click/Typer CLI framework patterns
- Rich library for terminal output
- IPython display protocol

### Background

The `burnt` what-if builder needs:
1. **CLI patterns** for building complex commands with scenarios
2. **Method aliases** for fluent builders (`.c()` vs `.cluster()`)
3. **Display patterns** for `compare()` and `display()` functions
4. **Output formatting** for terminal vs notebook

---

## Research Findings

### 1. CLI Patterns for Multi-Scenario Comparison

**Recommended: Hybrid Approach with Subcommands**

```bash
# Simple scenarios: flags for common options
burnt what-if "SELECT ..." --photon --instance Standard_DS3_v2

# Complex scenarios: nested subcommands for categories
burnt what-if "SELECT ..." cluster --photon --instance Standard_DS3_v2
burnt what-if "SELECT ..." data-source --to-delta --liquid-clustering "date,customer_id"
burnt what-if "SELECT ..." spark-config --shuffle-partitions 200 --aqe-enabled

# Multi-scenario comparison: separate commands with --scenario flag
burnt what-if "SELECT ..." --scenario baseline
burnt what-if "SELECT ..." --scenario photon --photon
burnt what-if "SELECT ..." --scenario downsized --instance Standard_DS3_v2
burnt what-if compare  # Compare all defined scenarios
```

**Patterns from other CLIs:**
- **AWS CLI**: Uses `--filters` and `--query` for complex filtering
- **gcloud**: Nested subcommands (e.g., `gcloud compute instances create`)
- **kubectl**: Subcommands with resource types (e.g., `kubectl get pods`)
- **terraform**: Single command with multiple flags (`terraform plan -var`)

**Key insights:**
- Chaining vs separate: Use chaining for Python API, separate commands for CLI
- Multi-scenario: `--scenario` flag to group configurations
- Subcommands provide clear categories without overwhelming flags

### 2. Method Aliases for Fluent Builders

**Recommended: Context-based shortcuts with explicit long names**

```python
# Builder pattern with aliases (following Pydantic/pandas conventions)
estimate.what_if().cluster().enable_photon().compare()

# Shortcuts for power users (optional, off by default)
estimate.what_if().c().p().compare()  # .c() = cluster(), .p() = enable_photon()

# Discoverability via __dir__ and help
estimate.what_if().options()  # Shows all available methods
```

**Alias conventions:**
- **Single-letter shortcuts**: Only for high-frequency methods (`.c()` for cluster, `.d()` for data_source, `.s()` for spark_config)
- **Underscore prefixes**: Internal methods use `_` prefix (e.g., `._validate()`)
- **Explicit over implicit**: Long names in examples/docs; shortcuts optional
- **Discovery pattern**: Implement `__dir__()` to show available methods

**Examples from other libraries:**
- **pandas**: `df.head()` (no alias needed - already short)
- **numpy**: `np.array()` vs `np.ndarray()` (alias `np` is module-level)
- **requests**: `requests.get()` (no aliases - clear verb-based API)
- **Pydantic**: Field aliases via `Field(alias="short_name")`

**Tradeoffs:**
- **Brevity vs discoverability**: Shortcuts save typing but hurt learnability
- **Consistency**: Single-letter aliases must be consistent across contexts
- **Recommendation**: Provide long names by default, shortcuts as opt-in

### 3. Alternative Patterns for `compare()` and `display()`

**Recommended: Dual-mode approach**

```python
# Option A: Top-level functions (Pythonic, explicit)
import burnt
result = burnt.compare(estimates=[estimate1, estimate2])
burnt.display(result)

# Option B: Method chaining (fluent, object-oriented)
result = estimate.what_if().cluster().enable_photon().compare()
result.display()

# Option C: Hybrid - top-level with optional chaining
# Method 1: Direct comparison function
burnt.compare(estimate.what_if().cluster().enable_photon())

# Method 2: Chaining with automatic comparison
estimate.what_if().cluster().enable_photon().display()  # Auto-compare single scenario

# Method 3: Comparison group builder
with burnt.comparison() as comp:
    comp.add(estimate.what_if().cluster().enable_photon(), name="Photon")
    comp.add(estimate.what_if().cluster().to_instance("Standard_DS3_v2"), name="Downsized")
comp.display()
```

**Tradeoffs:**
- **Top-level functions**: Better for functional programming, easier to test, more explicit
- **Method chaining**: Better for discovery in notebooks, fluent API feels natural
- **Hybrid**: Best of both worlds - `burnt.compare()` for explicit comparison, `.compare()` for fluent workflow

**Pythonic conventions:**
- `display()` as method follows IPython/Jupyter conventions
- `compare()` as function follows standard library patterns (`itertools.groupby()`, `functools.reduce()`)
- **Recommendation**: Implement both, document primary pattern clearly

### 4. CLI Command Structure

**Recommended: Nested subcommands with category prefixes**

```python
# CLI Structure in Typer/Click
@app.command()
def what_if(
    query: str,
    # Top-level common options
    output: str = "table",  # table, json, markdown
    scenario: str = None,   # Named scenario for comparison
    # Category flags (when simple)
    photon: bool = False,
    instance: str = None,
):
    pass

@app.command("what-if")
def what_if_cluster(
    query: str,
    photon: bool = False,
    instance: str = None,
    workers: int = None,
):
    """Optimize cluster configuration"""
    pass

@app.command("what-if")
def what_if_data_source(
    query: str,
    to_delta: bool = False,
    liquid_clustering: str = None,
    cache: bool = False,
):
    """Optimize data source layer"""
    pass
```

**Output formatting:**
```bash
# Terminal: Rich tables
burnt what-if "SELECT ..." --photon --output table

# JSON for automation
burnt what-if "SELECT ..." --photon --output json

# Markdown for notebooks/docs
burnt what-if "SELECT ..." --photon --output markdown
```

**Terminal vs Notebook:**
- **Terminal**: Use Rich tables for structured output
- **Notebook**: Use IPython display for rich HTML
- **Automation**: Use JSON output for CI/CD

### 5. Concrete Implementation Examples

**Python API (Primary):**
```python
import burnt

# Single scenario
result = (
    burnt.estimate("SELECT * FROM large_table")
    .what_if()
    .cluster()
    .enable_photon()
    .to_instance("Standard_DS3_v2")
    .compare()
)
result.display()  # Rich HTML table in notebook

# Multi-scenario comparison
comparison = (
    burnt.estimate("SELECT * FROM large_table")
    .what_if()
    .cluster().enable_photon()
    .what_if()
    .cluster().to_instance("Standard_DS3_v2")
    .compare()
)
comparison.display()
```

**CLI (Secondary):**
```bash
# Simple scenario
burnt what-if "SELECT *" --photon --instance Standard_DS3_v2

# Categorized scenarios
burnt what-if "SELECT *" cluster --photon --instance Standard_DS3_v2
burnt what-if "SELECT *" data-source --to-delta --liquid-clustering date

# Multi-scenario comparison
burnt what-if "SELECT *" --scenario baseline
burnt what-if "SELECT *" --scenario photon --photon
burnt what-if compare --output table
```

### 6. Key Design Decisions

1. **CLI Discovery**: Use subcommands for categories, flags for simple scenarios
2. **Method Aliases**: Optional single-letter shortcuts, explicit long names in docs
3. **Comparison Pattern**: Hybrid - `burnt.compare()` function + `.compare()` method
4. **Display Pattern**: `.display()` method for rich output, supports notebook/terminal
5. **Output Formats**: Rich tables (terminal/notebook), JSON (automation), Markdown (docs)
6. **Cost Transparency**: Inline verified/estimated flags with source citations

This design follows the "cuts like butter" principle: fluent interfaces for Python API, clear subcommands for CLI, and progressive disclosure of complexity.

---

## Acceptance Criteria

- [x] Researched CLI patterns from AWS, gcloud, kubectl, terraform
- [x] Researched method alias patterns from Python libraries
- [x] Researched compare() and display() patterns
- [x] Provided concrete examples for each area
- [x] Documented key design decisions
- [x] Created recommendations for burnt what-if builder

---

## Verification

### Commands

```bash
# No code changes - research only
# Review task file for completeness
cat tasks/r6-cli-patterns-research.md
```

### Integration Check

- [x] Research document created with concrete examples
- [x] Recommendations align with DESIGN.md "cuts like butter" principle
- [x] Patterns ready for implementation in s2-01a and s2-01b

---

## Handoff

### Result

Research completed. Recommendations:

1. **CLI**: Hybrid subcommand + flag approach with `--scenario` for multi-scenario
2. **Method Aliases**: Optional single-letter shortcuts, explicit long names default
3. **Compare/Display**: Hybrid pattern - `burnt.compare()` function + `.compare()` method
4. **Output**: Rich tables (terminal/notebook), JSON (automation), Markdown (docs)

Ready for implementation in Sprint 2 tasks.

```yaml
status: done
```
