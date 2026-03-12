# Task: CLI/Api Redesign - Burnt Init & Python API Cleanup

---

## Metadata

```yaml
id: s2-05-cli-api-redesign
status: todo
phase: 2
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Redesign burnt CLI and Python API for clean separation:
- **CLI**: Static analysis and tooling only (lint, init, tutorial, cache, rules)
- **Python API**: Runtime cost intelligence (estimate, advise, simulate)

### Files to read

```
# Required
src/burnt/__init__.py
src/burnt/cli/main.py
src/burnt/estimators/whatif.py
src/burnt/core/models.py
src/burnt/advisor/report.py

# Reference
DESIGN.md
tasks/s6-01-ast-lint-rules.md
```

---

## Part 1: CLI Commands (Keep)

| Command | Description |
|---------|-------------|
| `burnt check <path>` | Lint files for anti-patterns (rename from `lint`) |
| `burnt init` | Interactive project setup |
| `burnt tutorial` | Generate example notebooks in `examples/` |
| `burnt cache show` | Show cache contents |
| `burnt cache clear` | Clear cache |
| `burnt rules` | Interactive rules toggle (enable/disable rules) |
| `burnt --version` | Show version |

---

## Part 2: CLI Commands (Remove)

Remove these entirely (not just deprecate):
- `burnt estimate`
- `burnt what-if`
- `burnt advise`
- `burnt right-size`

---

## Part 3: Config File Support

### `.burnt.yaml` structure:
```yaml
workspace_url: "https://adb-123.azuredatabricks.net"

lint:
  fail_on: "error"  # info, warning, error
  exclude:
    - "tests/"
    - "*.ipynb"
  ignore_rules:
    - "select_star_no_limit"

cache:
  enabled: true
  ttl_seconds: 300
```

### `pyproject.toml` support:
```toml
[tool.burnt]
workspace_url = "https://adb-123.azuredatabricks.net"

[tool.burnt.lint]
fail_on = "error"
exclude = ["tests/"]
ignore_rules = []
```

### `burnt init` behavior:
1. Check for existing `.burnt.yaml` → if exists, prompt "Overwrite? [y/N]"
2. Check for `pyproject.toml` → prompt "Add [tool.burnt] section? [Y/n]"
3. Add entries to `.gitignore` for `.burnt/cache/`
4. Run `burnt tutorial` prompt at end: "Generate examples? [Y/n]"

---

## Part 4: Python API Changes

### Rename internal modules/classes:
- `src/burnt/estimators/whatif.py` → `simulation.py`
- `WhatIfBuilder` class → `Simulation` class
- `WhatIfResult` → `SimulationResult`
- `MultiScenarioResult` → `MultiSimulationResult`
- `WhatIfModification` → `SimulationModification`

### User-facing API changes:
- Remove `burnt.what_if(dbu, sku)` function
- Add `CostEstimate.simulate()` method (replaces `.what_if()`)
- Add `Simulation.add_scenario(name)` method
- Keep `burnt.estimate()` and `burnt.advise_current_session()` APIs

---

## Part 5: Cache Implementation

- Location: `.burnt/cache/` (project-local)
- Store: query fingerprints, DESCRIBE DETAIL results, etc.
- `burnt cache show`: List cache files and sizes
- `burnt cache clear`: Remove all cache files

---

## Part 6: Tutorial Generation

`burnt tutorial` creates:
```
examples/
├── 01_basic_estimation.ipynb
├── 02_advise_session.ipynb
├── 03_simulate_scenarios.ipynb
├── 04_lint_integration.ipynb
└── README.md
```

Each notebook demonstrates one workflow with comments.

---

## Part 7: Rules Command

`burnt rules` interactive TUI:
```
┌─────────────────────────────────────────────────────────┐
│                    Burnt Lint Rules                     │
├─────────────────────────────────────────────────────────┤
│  ✓ cross_join                 CROSS JOIN detection     │
│  ✓ select_star_no_limit       SELECT * without LIMIT   │
│  ✓ order_by_no_limit          ORDER BY without LIMIT   │
│  ✓ collect_without_limit      collect() without limit  │
│  ✓ python_udf                 Python UDF detection     │
│  ○ toPandas                   toPandas() detection     │
│  ○ repartition_one            repartition(1) warning   │
└─────────────────────────────────────────────────────────┘
[Enter] toggle rule  [A] enable all  [D] disable all  [Q] quit
```

---

## Acceptance Criteria

- [ ] `burnt check <path>` works (renamed from `lint`)
- [ ] `burnt init` works interactively, doesn't overwrite existing config
- [ ] `burnt tutorial` generates example notebooks in `examples/`
- [ ] `burnt cache show` and `burnt cache clear` work
- [ ] `burnt rules` interactive toggle works
- [ ] `burnt --version` works
- [ ] `burnt estimate`, `what-if`, `advise`, `right-size` commands removed
- [ ] `.burnt.yaml` config file works
- [ ] `pyproject.toml [tool.burnt]` support works
- [ ] `CostEstimate.simulate()` method works
- [ ] `Simulation.add_scenario(name)` method works
- [ ] All module/class renames complete (simulation.py, Simulation, etc.)
- [ ] All existing tests pass (352 tests)
- [ ] Lint passes (`uv run ruff check src/ tests/`)

---

## Verification

```bash
# Run all tests
uv run pytest -m unit -v

# Run lint
uv run ruff check src/ tests/

# Test each CLI command
burnt --version
burnt check ./src/
burnt init
burnt tutorial
burnt cache show
burnt cache clear
burnt rules
```

---

## Implementation Order

1. Update CLI (rename commands, remove old ones, add new ones)
2. Implement config file loading (`.burnt.yaml` + pyproject.toml)
3. Add cache commands
4. Add `burnt rules` command
5. Add `burnt tutorial` command
6. Rename simulation module and classes
7. Update Python API (simulate method, add_scenario)
8. Update exports in `__init__.py`
9. Update tests
10. Update DESIGN.md

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]