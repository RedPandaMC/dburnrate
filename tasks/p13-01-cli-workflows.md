# Task: Implement comprehensive CLI workflows and CI/CD templates

---

## Metadata

```yaml
id: p13-01-cli-workflows
status: todo
phase: 13
priority: medium
agent: ~
blocked_by: [p11-01-self-referential-estimation, p12-01-batch-glob-analysis]
created_by: planner
```

---

## Context

### Goal

Create comprehensive CLI workflow documentation and CI/CD integration templates. This includes GitHub Actions workflows, GitLab CI templates, pre-commit hooks, and Azure DevOps pipelines. The goal is to make it easy for teams to integrate cost estimation into their development workflows.

### Files to read (executor reads ONLY these)

```
# Required
docs/cli-workflows.md
src/dburnrate/cli/main.py

# Reference
DESIGN.md (Phase 13 section)
AGENTS.md
```

### Background

From DESIGN.md Phase 13:
- Document common CLI workflows
- Create GitHub Actions workflow templates
- Create GitLab CI templates
- Create pre-commit hook examples
- Document cost regression detection
- Document budget enforcement patterns
- Create PR comment bot example

This task is primarily documentation and template creation, building on the self-referential and batch features from phases 11 and 12.

---

## Acceptance Criteria

- [ ] Update `docs/cli-workflows.md` with comprehensive examples
- [ ] Create `.github/workflows/cost-check.yml` template
- [ ] Create `.github/workflows/cost-regression.yml` template
- [ ] Create `.github/workflows/budget-enforcement.yml` template
- [ ] Create `.gitlab-ci.yml` example
- [ ] Create `.pre-commit-config.yaml` example
- [ ] Create Azure DevOps pipeline example
- [ ] Document all output formats (JSON, CSV, table)
- [ ] Document troubleshooting common issues
- [ ] Document best practices for cost budgets
- [ ] Create example scripts for CI/CD integration
- [ ] All examples tested and working

---

## Verification

### Commands (run all, in order)

```bash
# Verify CLI works
uv run dburnrate --help
uv run dburnrate estimate --help
uv run dburnrate estimate-batch --help

# Verify docs are valid markdown
cat docs/cli-workflows.md | head -100

# Lint check
uv run ruff check src/ tests/
```

### Expected output

- All CLI commands show proper help text
- Documentation is complete and well-formatted
- All workflow templates are syntactically valid
- No lint errors

---

## Handoff

### Result

[Executor fills this in when done]

```
status: todo
```

### Blocked reason

[If blocked, explain what is missing]
