# Agent Instructions

This file guides AI coding agents working on the Transaction Audit System. It is inspired by the Karpathy-style LLM coding guidelines, but tailored to this repo.

## Project Goal

Build an explainable financial audit prototype that can:

- validate transaction files,
- reconcile transactions against reference files,
- produce reviewer-friendly audit outputs,
- grow in small, tested phases.

Use `README.md` for the current product scope and `AUDIT_RULES.md` for the current implemented checks. Do not duplicate phase status in this file; phase notes go stale quickly.

## Think Before Coding

- State assumptions when the request is ambiguous.
- Ask before making broad product or architecture changes.
- Push back if a simpler approach solves the same problem.
- Prefer the existing project shape over new frameworks.
- Treat audit correctness and reviewer clarity as product requirements, not polish.

## Simplicity First

- Do not add speculative abstractions.
- Do not introduce a rule registry, ML model, database, or React frontend unless explicitly planned.
- Keep deterministic, explainable rules ahead of AI/ML scoring.
- Keep near-term ML transaction-focused unless the user explicitly reopens employee or chart-of-account support.
- Keep ML/anomaly output explainable and separate from deterministic validation, reconciliation, and review-priority scoring. `scikit-learn` Isolation Forest is currently a supporting signal, not the source of fraud conclusions.
- If a change can be a focused function plus tests, do that before creating a framework.
- Avoid generic "enterprise" features unless they support the current audit workflow.

## Surgical Changes

- Touch only files needed for the current task.
- Do not rewrite adjacent code just because it could be cleaner.
- Do not remove unrelated code or comments.
- Remove dead code only when your own change made it dead, or when the user asked for cleanup.
- Preserve existing style and naming unless the task is specifically about refactoring.

## Architecture Rules

- Streamlit is a UI adapter only.
- Core logic belongs in `transaction_audit/`.
- Validation and reconciliation should be pure Python dataframe logic.
- Prefer functions that accept dataframes and return structured results.
- Keep uploaded datasets separate until a specific reconciliation rule needs a join.
- `AuditBundle` should receive canonical dataframes, not raw uploaded schemas.
- Schema mapping and import profiles are ingestion concerns, not validation logic.
- Audit findings must remain explainable to a human reviewer.

## Testing Expectations

For non-trivial changes:

1. Add or update focused tests for the behavior.
2. Run the full suite:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```

3. For app-level changes, verify import:

```powershell
.\.venv\Scripts\python.exe -c "import app; print('app import ok')"
```

4. For Phase 2 reconciliation changes, run the sample CLI:

```powershell
.\.venv\Scripts\python.exe -m transaction_audit.bundle_cli `
  --transactions sample_data\phase2_transactions.csv `
  --vendors sample_data\phase2_vendors.csv `
  --beneficiaries sample_data\phase2_beneficiaries.csv `
  --budget sample_data\phase2_budget.csv
```

5. Commit only when verification passes.

## Rule And Risk Documentation

- Keep `AUDIT_RULES.md` aligned with implemented rules.
- If you add a new `issue_type`, update reviewer guidance in `transaction_audit/audit.py`.
- New findings should include a clear title, why it matters, and recommended action.
- Do not let raw technical issue logs become the primary user-facing output.

## Review Scoring Boundary

Review-priority scoring:

```text
review_priority_band = worst signal wins
review_priority_score = additive weights capped at 100
review_priority_drivers = stable, human-readable reasons
```

The score must be deterministic. The same input data and issues must produce identical scores, bands, and drivers. Do not call it a fraud score or fraud probability.

Keep scoring logic in `transaction_audit/risk.py`. Do not add scoring rules to `transaction_audit/reconciliation.py`; reconciliation should keep producing factual issues, and risk should decide review priority.

## Git And Safety

- Work on `main` only when the user asks; otherwise create a short descriptive branch.
- Keep branch names simple and do not include tool/vendor names.
- Do not use destructive git commands unless the user explicitly asks.
- Do not install untrusted hooks, MCP servers, plugins, or global tools just because a trending repo suggests them.
- Do not commit secrets, private data, or user-provided financial files unless they are clearly intended sample fixtures.
