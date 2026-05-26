# Transaction Audit System

Python and Streamlit prototype for testing financial transaction files, reconciling reference data, and producing reviewer-friendly audit outputs.

## What It Does

- Loads transaction files from CSV or Excel.
- Preserves raw values during ingestion so validation can report parsing issues clearly.
- Auto-detects common column names and supports manual schema mapping.
- Applies conservative cleanup for bank-style transaction exports, including narration-based counterparty extraction.
- Saves dataset-scoped import profiles for repeat file formats.
- Runs single-file transaction validation checks.
- Runs multi-file reconciliation against vendors, beneficiaries, and budgets.
- Suggests possible vendor matches when exact vendor matching fails but a high-confidence fuzzy name match exists.
- Adds deterministic review-priority scores, bands, and drivers for exception review.
- Shows a review queue with filters for priority band, issue type, account, currency, and counterparty.
- Records skipped checks when optional reference files are missing.
- Produces reviewer-friendly audit workbooks with findings, risk levels, recommended actions, and raw evidence.

## Current Workflows

### Single-File Validation

Use this when you only have a transaction file.

Checks include:

- missing required columns
- missing required values
- invalid amounts
- zero or large amounts
- invalid dates
- unsupported currencies
- duplicate transaction IDs
- possible duplicate payments

Bank-style CSV exports with preamble rows, debit/credit split columns, narration fields, and footer totals can be cleaned before validation. See the realistic fixtures in `sample_data/realistic_bank_export.csv`, `sample_data/realistic_bank_export_29.csv`, `sample_data/realistic_mobile_money_export.csv`, and `sample_data/realistic_ledger_export.csv`.

### Multi-File Reconciliation

Use this when you have transactions plus reference files.

Supported files:

- transactions, required
- vendors/suppliers, optional
- beneficiaries, optional
- budget, optional

Checks include:

- transactions referencing vendors not found in the vendor file
- possible vendor matches that need human review
- transactions referencing beneficiaries not found in the beneficiary register
- duplicate vendor reference keys
- duplicate beneficiary reference keys
- beneficiaries paid more than their expected amount, when `expected_amount` is provided
- spend over approved budget lines, when a budget file is uploaded

Vendor concentration, threshold structuring, and timing anomaly checks are planned later.

## Audit Workbook

The multi-file audit workbook includes:

- `read_me`
- `summary`
- `review_findings`
- `finding_summary`
- `transaction_review`
- `dataset_summary`
- `issue_counts`
- `transaction_issues`
- `reference_issues`
- `raw_issues`
- `skipped_checks`
- `schema_mappings`
- uploaded input datasets such as `transactions`, `vendors`, `beneficiaries`, and `budget`

Start with `review_findings`. It is the human-facing sheet with review-priority score, band, drivers, review status, finding explanation, recommended action, and evidence. Use `transaction_review` when you want a row-level priority queue, including rows with no technical issue.

## Rules Reference

See [AUDIT_RULES.md](AUDIT_RULES.md) for the current rule list, risk levels, issue types, reviewer actions, skipped checks, and planned next rules.

## AI Agent Guidance

See [AGENTS.md](AGENTS.md) for repo-specific instructions for Codex, Claude, Cursor, and other coding agents. It captures the project rules around simplicity, surgical changes, testing, audit explainability, and the Phase 3 roadmap.

## Run Locally

Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run the Streamlit app:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Run tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```

Run the Phase 2 sample CLI:

```powershell
.\.venv\Scripts\python.exe -m transaction_audit.bundle_cli `
  --transactions sample_data\phase2_transactions.csv `
  --vendors sample_data\phase2_vendors.csv `
  --beneficiaries sample_data\phase2_beneficiaries.csv `
  --budget sample_data\phase2_budget.csv
```

## Design Principles

- Keep Streamlit as a UI adapter.
- Keep validation and reconciliation as pure Python dataframe logic.
- Prefer deterministic, explainable rules before ML or AI.
- Avoid silently dropping bad data; return audit issues or skipped checks.
- Keep uploaded datasets separate until a specific reconciliation rule needs a join.
