# Audit Rules and Risk Levels

Last updated: 2026-07-15

This file lists the checks currently implemented by the audit engine. It is meant to make the review scope clear for developers, testers, and non-technical reviewers.

## Review Priority

The workbook now includes deterministic review-priority fields:

| Field | Meaning |
| --- | --- |
| `review_priority_score` | Rule-based score from 0-100. Multiple drivers add together and cap at 100. |
| `review_priority_band` | Low, medium, or high. The highest-risk signal controls the band. |
| `review_priority_drivers` | Human-readable reasons behind the score. |

These scores are audit-support review drivers, not fraud probabilities or audit conclusions.
Low-priority pattern drivers, such as a one-off counterparty, are only applied when the file has enough rows for the pattern to be meaningful.
For bank-style exports, the cleaner extracts a concise counterparty from narration text and preserves the full source narration in `description` for evidence.

## Risk Levels

| Severity / Signal | Workbook priority | Meaning |
| --- | --- | --- |
| `error` | High | Blocking or high-risk issue. The record or reference should be reviewed before relying on the audit output. |
| `warning` | Medium | Review signal. The data may be valid, but the item deserves human attention. |

The workbook combines technical severities with issue-specific scoring rules in `review_findings` and `transaction_review`.

## ML / Anomaly Review

The `ml_anomalies` output is a separate anomaly-screening layer for review support. Its score is derived from a deterministic `scikit-learn` Isolation Forest; validation findings do not add points to the model score. It is not a fraud probability and should not be treated as an audit conclusion.

The model runs only with at least 50 transactions. It is configured to flag roughly the most unusual 5% of the uploaded population. A score of 50 or above means Isolation Forest classified the row as an outlier; scores below 50 are relative ranks among model inliers.

Current transaction-level drivers:

| Model feature / explanation | Meaning |
| --- | --- |
| Amount and amount rank | Amount is unusual compared with the uploaded transaction population. |
| Repeated invoice/reference | Invoice/reference and amount repeat for the same payee. |
| Missing invoice/reference | The transaction has no invoice/reference value where that field exists. |
| Payee frequency | The payee appears unusually often or rarely in the uploaded population. |

The labelled benchmark fixture is `sample_data/reduced_transactions_200_with_anomalies.csv`.

## Single-File Transaction Rules

These checks run against the transactions file.

| Rule | Issue type | Severity | What it detects | Reviewer action |
| --- | --- | --- | --- | --- |
| Required column check | `missing_column` | error | A required transaction column is not present. | Map the correct source column or request a corrected export. |
| Required value check | `missing_value` | error | A required field is blank on a transaction row. | Fill from source records or request a corrected export. |
| Amount parsing | `invalid_amount` | error | Amount cannot be parsed as money. | Correct the amount format and rerun the audit. |
| Zero amount | `zero_amount` | warning | Amount is exactly zero. | Confirm whether it is a reversal, placeholder, or export error. |
| Large amount | `large_amount` | warning | Amount meets/exceeds the configured large-amount threshold. | Check approval evidence and supporting documents. |
| Date parsing | `invalid_date` | error | Transaction date cannot be parsed. | Correct the date format and rerun the audit. |
| Currency scope | `unsupported_currency` | warning | Currency is outside the configured allowed list. | Confirm whether the currency is valid for this audit. |
| Missing invoice/reference | `missing_invoice_reference` | warning | Optional invoice/reference column exists but is blank for the transaction. | Request the invoice, voucher, or payment reference and document the review outcome. |
| Duplicate transaction ID | `duplicate_transaction_id` | error | Same transaction ID appears more than once. | Check whether rows are duplicates, corrections, or separate payments with wrong IDs. |
| Possible duplicate payment | `possible_duplicate_payment` | warning | When a consistent invoice total is available, positive payments for the same payee, invoice, and currency exceed that total when combined. Invoice matching ignores punctuation and spacing, normalises case, and treats leading-zero variants in each numeric section as equivalent. Without usable invoice totals, the fallback checks for the same invoice and amount or the same account/payee/amount/currency within the review window. | Compare the named payments with the invoice and payment evidence, then confirm whether every payment should exist. |
| Possible split payment | `possible_split_payment` | warning | Two or more positive payments have different transaction IDs but share a nonblank invoice/reference, payee, currency, and consistent invoice total. No individual payment equals the invoice total, but the payments reconcile to it when combined. Payment dates may differ. | Compare the named payments with the invoice and approval evidence, then confirm why the invoice was settled in parts. |
| Running balance integrity | `balance_movement_mismatch` | warning | A reported balance movement does not agree with the transaction amount after the statement sign convention is inferred from at least five reliable transitions. Ambiguous statements are not guessed. | Compare the row and preceding row with the source statement and check for omitted, duplicated, or incorrectly signed entries. |
| Payee concentration | `vendor_concentration` | warning | One payee represents at least the configured share of transaction value in a sufficiently large, varied currency population. Defaults: 40% share, at least 20 transactions, and at least 3 payees. Currencies are assessed separately. | Review procurement evidence, competitive sourcing, approvals, and the reason for the concentration. |

## Multi-File Reconciliation Rules

These checks run when reference files are uploaded with transactions.

| Rule | Issue type | Severity | What it detects | Reviewer action |
| --- | --- | --- | --- | --- |
| Unknown vendor reference | `unknown_vendor_reference` | error | Transaction references a vendor missing from the vendor file. | Confirm whether the vendor should be approved or investigate the payment. |
| Possible vendor match | `possible_vendor_match` | warning | Exact vendor match failed, but a similar vendor name was found above the confidence threshold. | Review the suggested match and confirm whether the transaction should be linked to that vendor. |
| Vendor ID/name mismatch | `vendor_reference_mismatch` | warning | Vendor ID is unknown, but the payee name exactly matches a different approved vendor. | Confirm the intended vendor and correct the transaction or vendor master reference. |
| Duplicate vendor key/name | `duplicate_vendor_reference` | error | Vendor file has duplicate normalized vendor IDs or names. | Correct or merge duplicate vendor master records. |
| Unknown beneficiary reference | `unknown_beneficiary_reference` | error | Transaction references a beneficiary missing from the beneficiary register. | Confirm beneficiary identity before approving payment. |
| Duplicate beneficiary key | `duplicate_beneficiary_reference` | error | Beneficiary file has duplicate beneficiary IDs. | Correct duplicate beneficiary records and check for overpayment risk. |
| Beneficiary overpayment | `beneficiary_overpaid` | error | Total payments to a beneficiary exceed `expected_amount` in the beneficiary register. | Confirm entitlement, approvals, and whether the excess payment should be recovered or explained. |
| Unknown budget line | `unknown_budget_line` | error | Transactions reference a budget line/category missing from the budget file. | Confirm the correct budget line or update the approved budget file. |
| Budget overspend | `budget_line_overspent` | error | Actual spend for a budget line exceeds `approved_amount`. | Review donor approval, budget amendments, and support for the overspend. |
| Duplicate budget key | `duplicate_budget_reference` | error | Budget file has duplicate budget line/category references. | Keep one authoritative row per budget reference. |

Beneficiary entitlement and budget comparisons use shared context fields when both files provide them: `currency`, `period`, `programme_id`, `project_id`, `donor`, and `location`. This prevents unrelated currencies, periods, programmes, or locations from being combined. The mapping layer recognizes common headers for these fields.

## Skipped Checks

Skipped checks are not findings. They tell reviewers which optional checks did not run.

| Check | When skipped | Meaning |
| --- | --- | --- |
| Vendor reconciliation | Vendors file not uploaded or no usable vendor columns found. | Vendor reference assurance is incomplete. |
| Beneficiary reconciliation | Beneficiary file not uploaded or `beneficiary_id` is missing. | Beneficiary reference assurance is incomplete. |
| Budget reconciliation | Budget file not uploaded, no usable budget line/category exists, or approved amounts are missing. | Budget-vs-actual assurance is incomplete. |

## Current Scope Limitations

The system currently answers:

- whether transaction data is usable,
- whether common transaction quality issues exist,
- whether vendors and beneficiaries referenced by transactions exist in uploaded reference files,
- whether unmatched vendor references have a likely fuzzy name match that needs review,
- whether reference files contain duplicate keys.
- whether beneficiaries received more than expected.
- whether spend exceeds approved budget lines.
- whether running balances reconcile when the statement provides enough evidence to infer its sign convention.
- whether one payee represents a high share of transaction value within a currency population.
- whether partial payments for one payee and invoice reconcile to the invoice total.
- which findings and transaction rows should be reviewed first using deterministic priority scores.
- which transactions look unusual compared with the uploaded transaction population, using the separate ML/anomaly review output.

It does not establish that the uploaded population is complete unless a usable running balance is present. It also does not inspect approvals or source documents, confirm bank balances externally, test segregation of duties, or conclude whether fraud occurred.

## Planned Next Rules

| Planned rule | Risk question |
| --- | --- |
| Broader threshold structuring | Are payments without a shared invoice clustered just below review thresholds? |
| Timing anomalies | Are payments clustered around weekends, month-end, or project closeout? |
