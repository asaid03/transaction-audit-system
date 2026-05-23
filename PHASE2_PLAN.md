# Phase 2 Plan: Multi-File Reference Reconciliation

## Goal

Phase 2 turns the project from a single-file transaction validator into a multi-file audit prototype.

The focus is reference reconciliation for NGO and small-business audit work:

- payments to approved vendors or suppliers
- payments to expected beneficiaries
- spending against approved budget lines

This is not a generic enterprise AP system yet. The MVP should prove that uploaded files can be mapped, reconciled, and turned into audit-ready review output without making the codebase abstract or brittle.

## Scope

Required:

- `transactions`

Supported reference files:

- `vendors`
- `beneficiaries`
- `budget`

Reference files are optional individually, but Phase 2 should clearly report which files were uploaded and which checks were skipped.

The practical rule is:

- transactions are required
- at least one reference file should be uploaded to run cross-file checks

## Why This Shape

The earlier shelved multi-file work used a more corporate bundle:

- transactions
- vendors
- employees
- chart of accounts

That is useful later, but too ambitious for the MVP. For the target audit context, beneficiary and budget checks are more relevant than employee and chart-of-accounts checks.

Keep from the stash:

- file-type-aware schemas
- file-type-aware profiles
- reusable mapping flow ideas

Do not blindly restore from the stash:

- four-file required upload flow
- approver/self-approval checks
- employee/account checks as MVP requirements
- large Streamlit-first orchestration

## Architecture

Add three modules:

```text
transaction_audit/
  datasets.py
  joins.py
  reconciliation.py
```

### `datasets.py`

Owns dataset definitions and bundle structures.

Expected concepts:

- `DatasetType`
- `AuditDataset`
- `AuditBundle`
- required fields per dataset type
- optional fields per dataset type
- dataset availability summary

The bundle should keep each file separate. Do not merge everything into one giant dataframe early.

### `joins.py`

Owns safe join/reconciliation helpers.

Expected helpers:

- normalize join keys
- preserve source row numbers
- find missing references with `indicator=True`
- enforce expected cardinality with `validate="many_to_one"` where appropriate

These helpers should be tested in isolation.

### `reconciliation.py`

Owns cross-file audit checks.

Initial checks:

- transaction vendor/counterparty not found in vendor list
- transaction beneficiary not found in beneficiary register
- beneficiary paid more than expected amount, if expected amount exists
- duplicate beneficiary identifiers, phone numbers, or payment accounts, if fields exist
- spending category/budget line not found in budget file
- spending over budget line, if budget amount exists
- duplicate vendor identifiers or names
- vendor missing useful reference fields, if those columns exist

## Issue Model

Extend `ValidationIssue` before building reconciliation.

Current issues are implicitly about transactions. Phase 2 needs dataset-aware issues.

Add:

```python
dataset: str = "transactions"
related_dataset: str | None = None
related_record_id: str | None = None
```

The issue dataframe should make cross-file issues clear:

```text
dataset              transactions
row_number           12
transaction_id       TXN-102
field                beneficiary_id
issue_type           unknown_beneficiary
severity             error
related_dataset      beneficiaries
related_record_id    BEN-404
message              Beneficiary BEN-404 was paid but is not in the beneficiary register.
```

Phase 1 rules should continue to default to `dataset="transactions"` so the single-file validator keeps working.

## UI

Streamlit should remain an adapter.

Phase 2 UI should add a multi-file section:

- required transactions uploader
- optional vendors uploader
- optional beneficiaries uploader
- optional budget uploader

Each uploaded file gets:

- schema mapping
- optional saved profile
- mapping preview

The UI should show skipped checks clearly:

```text
Vendors uploaded: yes
Beneficiaries uploaded: no, beneficiary checks skipped
Budget uploaded: yes
```

## Audit Workbook

Add a bundle workbook export.

Expected sheets:

- `run_summary`
- `dataset_summary`
- `issues`
- `issue_counts`
- `transactions`
- `vendors`, if uploaded
- `beneficiaries`, if uploaded
- `budget`, if uploaded
- `schema_mappings`
- `skipped_checks`

## Implementation Sequence

1. Extend `ValidationIssue` with dataset-related fields.
2. Add tests proving Phase 1 output still works.
3. Add `datasets.py`.
4. Add `joins.py` with isolated tests.
5. Add `reconciliation.py` with vendor checks first.
6. Add beneficiary checks.
7. Add budget checks.
8. Add bundle workbook export.
9. Add Streamlit multi-file UI last.

## Non-Goals For Phase 2

- no ML or risk scoring
- no database
- no authentication
- no React rewrite
- no full rule registry
- no employee/vendor conflict checks unless the basic reconciliation shape is already stable
- no chart-of-accounts workflow in the MVP

## Success Criteria

Phase 2 is successful when a user can:

1. Upload a transaction file.
2. Upload at least one reference file.
3. Map each file into a canonical schema.
4. Run cross-file checks.
5. See which checks ran and which were skipped.
6. Download an audit workbook that explains the findings clearly.
