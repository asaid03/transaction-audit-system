# Phase 3 Plan

Last updated: 2026-05-25

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

Add a dedicated determinism test before wiring the score into exports.

## Initial Signals

High-priority examples:

- unknown vendor reference
- beneficiary overpaid
- budget line overspent
- duplicate transaction ID
- missing required field
- invalid amount or date

Medium-priority examples:

- possible duplicate payment
- possible fuzzy vendor match
- large amount transaction
- unsupported currency

Low-priority examples:

- zero amount
- round-number payment
- weekend or month-end timing
- rare counterparty

Start with existing validation and reconciliation issues before adding new pattern signals.

## Build Order

1. Add `transaction_audit/risk.py`.
2. Score existing validation and reconciliation issues.
3. Add tests for high, medium, low, cumulative score, worst-band behavior, and determinism.
4. Add score, band, and drivers to `review_findings`.
5. Add score fields to the audit workbook.
6. Add minimal Streamlit sorting/filtering by priority.

## Later

After the basic scoring layer is stable:

- add round-number, weekend, month-end, rare counterparty, and threshold-adjacent signals
- redesign the UI around an exception review queue
- add review workflow and SQLite persistence only after review status/notes become useful
