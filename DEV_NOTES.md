# Development Notes

Internal project notes for keeping context between sessions.

## Current State

- Active branch: `main`
- Remote: `origin/main`
- Phase 1 hardening has been merged into `main`.
- Phase 2 multi-file audit work is shelved, not discarded.

## Phase 1 Capabilities

- CSV/Excel ingestion with raw string preservation.
- Flexible schema inference and manual column mapping.
- Saved import profiles in `import_profiles/`.
- Pure Python validation logic separate from Streamlit.
- Basic validation checks:
  - missing required columns
  - missing required values
  - invalid amount
  - zero amount
  - large amount
  - invalid date
  - unsupported currency
  - duplicate transaction ID
- Audit summary generation.
- Streamlit UI for upload, mapping, review, and CSV downloads.
- Regression tests with `unittest`.

## Useful Commands

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
.\.venv\Scripts\python.exe -m transaction_audit.validation sample_data\sample_transactions.csv
.\.venv\Scripts\python.exe -m streamlit run app.py
```

## Shelved Work

Phase 2 work was stashed before returning to Phase 1:

```text
stash@{0}: On multi-file-audit: shelve multi-file audit work
```

It included early multi-file audit bundle support:

- file-type-aware schemas
- profiles for transactions/vendors/employees/chart of accounts
- reference checks for unknown vendors, approvers, employees, accounts, approval limits, and self-approval
- multi-file Streamlit upload flow

Do not apply this stash blindly. Review it and rebuild intentionally when Phase 2 starts again.

## Next Sensible Steps

1. Add probable duplicate payment detection inside Phase 1.
2. Add issue explanations that are clearer for non-technical reviewers.
3. Consider CSV export formula-injection sanitization before wider use.
4. Return to multi-file audit only after single-file rules feel solid.

## Design Principles

- Keep Streamlit as a UI adapter only.
- Core logic should accept dataframes and return structured results.
- Prefer deterministic, explainable rules before ML/AI.
- Avoid silently dropping bad data; return audit issues instead.
- Keep profiles as import-layer concerns, not validation logic.
