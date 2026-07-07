# Phase 3 Plan

Last updated: 2026-07-07

## Current Status

Phase 3 review-priority scoring is implemented on `phase-3-review-priority`:

- `transaction_audit/risk.py` scores existing validation and reconciliation issues.
- `review_findings` includes deterministic score, band, and drivers.
- Workbooks include a `transaction_review` sheet for row-level prioritisation.
- Bank-style narration text is parsed into cleaner counterparty values while preserving the full narration in `description`.
- Determinism coverage is in `tests/test_risk.py`.
- The Streamlit UI has been refactored into smaller UI modules and is acceptable as a prototype UI adapter, not the long-term frontend.

## Goal

Add a deterministic review-priority layer that helps auditors decide which exceptions to review first.

This is audit support, not a fraud model. Use language such as:

- review priority
- exception review
- audit support
- priority drivers

Avoid:

- fraud score
- fraud probability
- automated auditor

## Scoring Model

Use a hybrid scoring approach:

- `review_priority_band`: worst signal wins.
- `review_priority_score`: additive weights capped at 100.
- `review_priority_drivers`: stable, readable reasons.

This keeps the band audit-defensible while still letting the score rank items within the same band.

## Determinism Requirement

The same inputs must produce identical outputs every time:

- same score
- same band
- same driver order

The first determinism test is implemented in `tests/test_risk.py`.

## Initial Signals

High-priority examples:

- unknown vendor reference
- beneficiary overpaid
- budget line overspent
- duplicate transaction ID
- missing required field
- invalid amount or date

Medium-priority examples:

- possible fuzzy vendor match
- large amount transaction
- unsupported currency

Low-priority examples:

- zero amount
- rare counterparty in files with enough rows for that signal to be meaningful

Possible duplicate payment is currently treated as high because duplicate-looking payments are a direct exception-review item.

## Build Order

1. Add `transaction_audit/risk.py`. Done.
2. Score existing validation and reconciliation issues. Done.
3. Add tests for high, medium, low, cumulative score, worst-band behavior, and determinism. Done.
4. Add score, band, and drivers to `review_findings`. Done.
5. Add score fields to the audit workbook. Done.
6. Add minimal Streamlit sorting/filtering by priority. Done.

## Next

Before adding ML, keep the input contract simple and transaction-focused:

- do not add employee or chart-of-account support just to satisfy one synthetic file shape
- keep ML/anomaly detection separate from validation, reconciliation, and scoring
- train or calibrate only against clearly labelled sample rows or reviewed outcomes
- export ML signals as explainable review drivers, not fraud probabilities

## Later

After the basic scoring layer is stable:

- add round-number, weekend, month-end, and threshold-adjacent rule signals
- add review workflow and SQLite persistence only after review status/notes become useful
