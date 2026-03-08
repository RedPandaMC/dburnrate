# tasks/ — Agentic Task Queue

This directory is the **handoff protocol** between Planner agents (Anthropic/Claude) and Executor agents (Kimi/Minimax/Sonnet).

---

## How it works

```
Planner                          Executor                       Validator
  |                                 |                               |
  |-- creates task file ----------> |                               |
  |   status: todo                  |                               |
  |                                 |-- claims task                 |
  |                                 |   status: in-progress         |
  |                                 |   agent: kimi-128k            |
  |                                 |                               |
  |                                 |-- implements code             |
  |                                 |-- runs tests/lint             |
  |                                 |-- runs integration checks     |
  |                                 |-- writes handoff notes        |
  |                                 |   status: validation-pending  |
  |                                 |                               |
  |                                 |-----------------------------> |
  |                                 |                               |-- runs benchmarks
  |                                 |                               |-- checks cost formulas
  |                                 |                               |-- marks status: done
  |<-- reads handoff notes ---------|<------------------------------|
  |-- archives to .md.completed     |                               |
```

---

## Task File Format

Every task is a single Markdown file. Use `TEMPLATE.md` to create new tasks.

### Status values

| Status | Meaning |
|--------|---------|
| `todo` | Ready to be picked up by an executor |
| `in-progress` | Claimed by an executor (check `agent` field) |
| `validation-pending` | Executor finished, waiting for validator |
| `done` | Validated and completed — handoff notes written |
| `blocked` | Cannot proceed — reason in `handoff.blocked_reason` |
| `cancelled` | No longer needed |

### Naming convention

```
{phase}-{sequence}-{short-slug}.md

Examples:
  p4a-01b-remaining-bugs.md
  p5-01-job-definition-cost.md
  p6-02-cost-regression.md
```

---

## Parallel execution rules

1. **Check `blocked_by`** before starting — wait for dependencies
2. **Check `context.files`** — never modify a file owned by another `in-progress` task
3. **One task at a time per agent** — finish or block before picking up another
4. **Atomic file ownership** — if two tasks touch the same file, the second must list `blocked_by` the first

---

## Planner checklist (before creating a task)

- [ ] Task is atomic — one logical unit of work
- [ ] `context.files` lists every file the executor must read (no extras)
- [ ] `context.goal` is unambiguous — an executor with no other context can understand it
- [ ] `acceptance_criteria` are testable — yes/no, not subjective
- [ ] **Estimation tasks**: Specifies formulas, constant derivations, and error bounds
- [ ] `verification.commands` are exact shell commands that will pass when done
- [ ] `blocked_by` lists any task IDs that must complete first

---

## Executor checklist (before marking validation-pending)

- [ ] All `verification.commands` ran and passed (paste output in `handoff.result`)
- [ ] New/modified code has type hints and docstrings on public functions
- [ ] No new lint errors: `uv run ruff check src/ tests/`
- [ ] Integration check: Confirmed the new code is actually reachable/working via CLI
- [ ] `handoff.result` summarizes what was changed and why
- [ ] If blocked, `handoff.blocked_reason` explains exactly what is needed to unblock

---

## Validator checklist (before marking done)

- [ ] Run benchmark dataset (if applicable) and confirm estimates meet phase tolerance (10x, 3x, 2x)
- [ ] Verify mathematical constants are justified
- [ ] Archive task file to `.md.completed`
