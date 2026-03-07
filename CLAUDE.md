# CLAUDE.md — dburnrate Agent Rules

> Auto-loaded by Claude Code. Also governs all opencode sessions in this repo.
> Full extended rules: see AGENTS.md. Roadmap: FUTURE_TODOS.md.

---

## Project At a Glance

**dburnrate** — Python package for pre-execution Databricks cost estimation.
- Stack: Python 3.12, uv, hatchling, pydantic v2, typer, rich, sqlglot
- Source: `src/dburnrate/` | Tests: `tests/unit/` | CLI: `uv run dburnrate`
- Status: MVP built (phases 0-4 of PLAN.md). Next: fix tests/lint, then system tables.

---

## Tooling Commands (memorize these)

```bash
uv run pytest -m unit -v               # unit tests
uv run ruff check src/ tests/          # lint
uv run ruff format --check src/ tests/ # format check
uv run ruff format src/ tests/         # auto-format
uv run bandit -c pyproject.toml -r src/ # security
uv run dburnrate --help                # CLI
```

---

## Agentic Workflow — READ THIS FIRST

This repo uses **two-role parallel coding**:

| Role | Model | Responsibility |
|------|-------|---------------|
| **Planner** | Anthropic (Claude Opus/Sonnet) | Reads FUTURE_TODOS.md, decomposes tasks, writes task specs to `tasks/` |
| **Executor** | Kimi / Minimax / Sonnet | Picks up task files from `tasks/`, implements code, runs tests, writes handoff |

### If you are a PLANNER agent:
1. Read FUTURE_TODOS.md — work Phase 1 first, then Phase 2, etc.
2. Check `tasks/` for existing in-progress/blocked tasks before creating new ones
3. Create self-contained task files using `tasks/TEMPLATE.md`
4. Tasks must include: file list to read, exact acceptance criteria, test commands
5. Mark tasks `status: todo` — executor picks them up
6. After executor marks `status: done`, verify the handoff notes, update FUTURE_TODOS.md

### If you are an EXECUTOR agent:
1. List `tasks/` and pick ONE `status: todo` task
2. Update status to `status: in-progress` and add `agent: <your-model-id>`
3. Read ONLY the files listed in the task's `context.files` section
4. Implement, then run the task's `verification.commands` — ALL must pass
5. Write results to `handoff.result` in the task file
6. Mark `status: done` or `status: blocked` (with reason)
7. Never start a second task until the first is done or blocked

### Parallel execution rules:
- Tasks with non-overlapping file sets can run in parallel
- Tasks with `blocked_by: [task-id]` must wait
- Never modify a file another in-progress task owns (check `context.files`)

### Task file lifecycle (MANDATORY)
- Active tasks: `tasks/<id>.md` (status: todo | in-progress | blocked)
- Completed tasks: rename to `tasks/<id>.md.completed` — **never delete, always rename**
- When marking a task done, the planner renames the file: `mv tasks/<id>.md tasks/<id>.md.completed`
- `ls tasks/*.md` should show only actionable tasks; `ls tasks/*.md.completed` shows history

---

## Code Standards (non-negotiable)

1. **Type hints** on all public functions — `def foo(x: int) -> str:`
2. **Docstrings** on all public functions (one-liner minimum)
3. **No bare except** — always catch specific exceptions
4. **Conventional commits** — `feat:`, `fix:`, `refactor:`, `test:`, `docs:`
5. **Never commit** without all tests passing and lint clean

---

## Key Architecture

```
src/dburnrate/
├── core/          # models.py, config.py, pricing.py, exchange.py, exceptions.py
├── parsers/       # sql.py (sqlglot), pyspark.py (ast), antipatterns.py, notebooks.py
├── estimators/    # static.py (complexity→DBU), whatif.py (scenario modeling)
├── cli/           # main.py (typer + rich)
└── forecast/      # prophet.py (post-MVP)
```

Critical research findings (from RESEARCH.md):
- `EXPLAIN COST` is the strongest cold-start signal (no execution needed)
- Delta `_delta_log` provides exact file sizes without scanning
- `system.query.history` fingerprinting = highest-ROI for recurring queries
- Serverless bills per warehouse uptime, NOT per query

---

## Current Priority Order

Phases 1–3 complete. Phase 4 (Delta + EXPLAIN wired into CLI) is next.
See `tasks/*.md` for active tasks, `tasks/*.md.completed` for history.

---

## Anti-patterns to avoid in this codebase

- No hardcoded API keys or workspace URLs
- No `SELECT *` in system table queries (explicit columns only)
- No synchronous HTTP in hot paths (use async or cache)
- No `float` for money — use `Decimal` for all USD/EUR amounts
- No broad `except Exception` without re-raising or logging
