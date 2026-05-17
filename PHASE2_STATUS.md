# Phase 2 Status

Last updated: 2026-05-17

## Branch

Current branch:

```text
phase2-multi-file-audit
```

This branch is for multi-file reference reconciliation.

## Current Goal

Phase 2 is building multi-file audit support for NGO and small-business style audits.

Current scope:

- required transaction file
- optional vendors/suppliers file
- optional beneficiaries file
- budget support planned, not exposed in the UI yet

The current Phase 2 focus is reference reconciliation:

```text
transactions.vendor_id      -> vendors.vendor_id
transactions.beneficiary_id -> beneficiaries.beneficiary_id
```

## What Works

### Core

- `ValidationIssue` is dataset-aware:
  - `dataset`
  - `related_dataset`
  - `related_record_id`
- `AuditBundle` keeps each uploaded dataset separate.
- `joins.py` provides normalized safe joins and source row-number preservation.
- `joins.py` rejects input files that already contain reserved internal join columns.
- `bundle_workflow.py` runs the combined Phase 1 validation + Phase 2 reconciliation flow outside Streamlit.
- `reconciliation.py` checks:
  - unknown vendor references
  - unknown beneficiary references
  - duplicate vendor reference keys
  - duplicate beneficiary reference keys
  - skipped checks for missing optional reference files
- `bundle_cli.py` runs Phase 2 reconciliation from PowerShell.

### Streamlit

`app.py` now has two workflow modes:

- `Single-file validation`
- `Multi-file reconciliation`

Multi-file mode currently supports:

- transactions upload
- vendors/suppliers upload
- beneficiaries upload

It shows:

- dataset summary
- transaction validation issues
- reference issues
- skipped checks
- input data previews
- combined issue CSV download
- transaction issues CSV download
- reference issues CSV download

Budget upload is intentionally not shown yet because budget reconciliation is not implemented.

## Sample Files

Phase 2 sample bundle:

```text
sample_data/phase2_transactions.csv
sample_data/phase2_vendors.csv
sample_data/phase2_beneficiaries.csv
```

Expected sample findings:

- `TXN-2005` has a zero amount transaction-validation warning
- `VEN-404` missing from vendors
- `BEN-404` missing from beneficiaries
- budget check skipped because no budget file is uploaded

## Commands

Run all tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```

Run Phase 2 CLI sample:

```powershell
.\.venv\Scripts\python.exe -m transaction_audit.bundle_cli `
  --transactions sample_data\phase2_transactions.csv `
  --vendors sample_data\phase2_vendors.csv `
  --beneficiaries sample_data\phase2_beneficiaries.csv
```

Run Streamlit:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Then open:

```text
http://localhost:8501
```

## Current Verification

Latest verified test run:

```text
Ran 42 tests
OK
```

Latest verified CLI sample:

```text
Reference issues found: 2
Skipped checks: 1
```

Latest verified UI import:

```text
app import ok
```

## Important Design Decisions

- Keep Phase 1 single-file validation intact.
- Phase 2 wraps around Phase 1 rather than replacing it.
- Keep Streamlit thin.
- Keep datasets separate; do not merge everything into one giant dataframe early.
- Report skipped checks explicitly instead of silently ignoring missing optional files.
- Report duplicate keys in reference files as audit issues instead of letting pandas join errors leak to users.
- Do not expose budget upload until budget reconciliation exists.

## Next Step

Phase 1 transaction validation is now wired into multi-file mode.

Current multi-file mode output:

```text
Multi-file mode output =
  Phase 1 transaction validation issues
  + Phase 2 reference reconciliation issues
  + skipped checks
```

Next desired implementation slice:

```text
Add a multi-file audit workbook/export that includes:
  dataset_summary
  transaction_issues
  reference_issues
  skipped_checks
  input datasets
```

Do not start budget checks or schema-profile expansion before this is done.

## Useful Recent Commits

```text
d6fd2ac Add Phase 2 reconciliation UI mode
f392ccf Clean up Phase 2 reconciliation loading
15831e1 Add Phase 2 sample reconciliation CLI
5cbcf60 Add Phase 2 reconciliation foundation
```
