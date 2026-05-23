# Audit Rules and Risk Levels

Last updated: 2026-05-23

This file lists the checks currently implemented by the audit engine. It is meant to make the review scope clear for developers, testers, and non-technical reviewers.

## Risk Levels

| Severity | Workbook priority | Meaning |
| --- | --- | --- |
| `error` | High | Blocking or high-risk issue. The record or reference should be reviewed before relying on the audit output. |
| `warning` | Medium | Review signal. The data may be valid, but the item deserves human attention. |

The workbook converts technical severities into reviewer-facing priorities in `review_findings`.

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
| Duplicate transaction ID | `duplicate_transaction_id` | error | Same transaction ID appears more than once. | Check whether rows are duplicates, corrections, or separate payments with wrong IDs. |
| Possible duplicate payment | `possible_duplicate_payment` | warning | Same account, counterparty, amount, and currency appears within the configured date window under different IDs. | Compare the payments and confirm both should exist. |

## Multi-File Reconciliation Rules

These checks run when reference files are uploaded with transactions.

| Rule | Issue type | Severity | What it detects | Reviewer action |
| --- | --- | --- | --- | --- |
| Unknown vendor reference | `unknown_vendor_reference` | error | Transaction references a vendor missing from the vendor file. | Confirm whether the vendor should be approved or investigate the payment. |
| Duplicate vendor key | `duplicate_vendor_reference` | error | Vendor file has duplicate vendor IDs or duplicate matched vendor keys. | Correct or merge duplicate vendor master records. |
| Unknown beneficiary reference | `unknown_beneficiary_reference` | error | Transaction references a beneficiary missing from the beneficiary register. | Confirm beneficiary identity before approving payment. |
| Duplicate beneficiary key | `duplicate_beneficiary_reference` | error | Beneficiary file has duplicate beneficiary IDs. | Correct duplicate beneficiary records and check for overpayment risk. |

## Skipped Checks

Skipped checks are not findings. They tell reviewers which optional checks did not run.

| Check | When skipped | Meaning |
| --- | --- | --- |
| Vendor reconciliation | Vendors file not uploaded or no usable vendor columns found. | Vendor reference assurance is incomplete. |
| Beneficiary reconciliation | Beneficiary file not uploaded or `beneficiary_id` is missing. | Beneficiary reference assurance is incomplete. |
| Budget reconciliation | Budget file not uploaded or budget logic not implemented yet. | Budget-vs-actual assurance is not available yet. |

## Current Scope Limitations

The system currently answers:

- whether transaction data is usable,
- whether common transaction quality issues exist,
- whether vendors and beneficiaries referenced by transactions exist in uploaded reference files,
- whether reference files contain duplicate keys.

It does not yet answer whether spending is reasonable against an approved budget. That belongs to the next planned slice: budget reconciliation.

## Planned Next Rules

| Planned rule | Risk question |
| --- | --- |
| Budget vs actual | Did spending exceed approved budget lines? |
| Beneficiary entitlement | Did beneficiaries receive the expected amount? |
| Vendor concentration | Is too much spend going to one vendor? |
| Threshold structuring | Are payments clustered just below review thresholds? |
| Timing anomalies | Are payments clustered around weekends, month-end, or project closeout? |
