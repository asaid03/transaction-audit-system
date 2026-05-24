# Phase 2 Status

Last updated: 2026-05-24

## Status

Phase 2 is complete and merged into `main`.

Phase 2 turned the project from a single-file transaction validator into a multi-file audit prototype with reviewer-friendly outputs.

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

Budget support is planned next but is not implemented yet.

## Current Multi-File Checks

```text
transactions.vendor_id      -> vendors.vendor_id
transactions.beneficiary_id -> beneficiaries.beneficiary_id
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
- automatic schema detection
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
- uploaded input datasets such as `transactions`, `vendors`, and `beneficiaries`

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
Ran 58 tests
OK
```

Also verified:

```text
app import ok
python -m compileall app.py transaction_audit tests
OK
Phase 2 sample CLI
OK
```

## Next Phase

Budget reconciliation and beneficiary entitlement checks are now implemented.

The next planned slice is spending-pattern anomaly checks:

- vendor concentration
- threshold structuring
- unusual timing such as weekend, month-end, or project-closeout clusters

Do not add ML or opaque risk scoring before deterministic anomaly rules are stable.
