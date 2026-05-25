# Phase 2 Status

Last updated: 2026-05-25

## Status

Phase 2 and Phase 2.5 are complete and merged into `main`.

Phase 2 turned the project from a single-file transaction validator into a multi-file audit prototype with reviewer-friendly outputs. Phase 2.5 added conservative cleaning for realistic bank, mobile-money, and ledger-style transaction exports.

## Completed Scope

- Required transaction file.
- Optional vendors/suppliers file.
- Optional beneficiaries file.
- Auto-detect-first schema mapping per uploaded dataset.
- Dataset-scoped import profiles.
- Vendor and beneficiary reference reconciliation.
- Conservative fuzzy vendor-name fallback for unmatched vendor references.
- Beneficiary entitlement check using `expected_amount`.
- Budget reconciliation using budget line/category actuals vs approved amounts.
- Duplicate vendor and beneficiary key checks.
- Explicit skipped-check reporting.
- Multi-file Streamlit workflow.
- Reviewer-friendly audit workbook.
- Audit rules/risk reference in `AUDIT_RULES.md`.
- Conservative bank/export cleaning when normal schema detection fails.
- Realistic messy transaction fixtures for bank, mobile-money, and ledger exports.

## Current Multi-File Checks

```text
transactions.vendor_id      -> vendors.vendor_id
transactions.beneficiary_id -> beneficiaries.beneficiary_id
transactions.budget_line_id -> budget.budget_line_id
```

Current reconciliation findings:

- unknown vendor reference
- possible vendor match
- unknown beneficiary reference
- beneficiary overpaid
- unknown budget line
- budget line overspent
- duplicate budget reference
- duplicate vendor reference
- duplicate beneficiary reference
- skipped optional checks

## Streamlit Behavior

The app has two workflow modes:

- `Single-file validation`
- `Multi-file reconciliation`

Multi-file mode supports:

- transactions upload
- vendors/suppliers upload
- beneficiaries upload
- budget upload
- automatic schema detection
- bank/export cleaning fallback for transaction files
- manual mapping only when needed or requested
- audit workbook download
- reviewer-facing findings table
- expandable details for mappings, raw issue tables, skipped checks, and input previews

## Workbook Output

Multi-file workbook sheets:

- `read_me`
- `summary`
- `review_findings`
- `finding_summary`
- `dataset_summary`
- `issue_counts`
- `transaction_issues`
- `reference_issues`
- `raw_issues`
- `skipped_checks`
- `schema_mappings`
- uploaded input datasets such as `transactions`, `vendors`, `beneficiaries`, and `budget`

`review_findings` is the primary sheet for humans. It includes:

- finding ID
- priority
- review status
- plain-language finding title
- why the issue matters
- recommended action
- affected dataset/row/record evidence
- review notes

## Verification

Latest verified state:

```text
Ran 70 tests
OK
```

Also verified:

```text
app import ok
python -m compileall app.py transaction_audit tests
OK
Phase 2 sample CLI
OK
Realistic cleaning fixtures
OK
```

## Next Phase

The next planned slice is Phase 3 review-priority scoring:

- add `transaction_audit/risk.py`
- generate deterministic `review_priority_score`, `review_priority_band`, and `review_priority_drivers`
- use worst-signal-wins for the band and additive capped weights for the raw score
- add a determinism test: the same inputs must produce identical scores and drivers
- keep the wording as review priority, not fraud probability

Do not add ML or opaque risk scoring before reviewed outcome data exists.
