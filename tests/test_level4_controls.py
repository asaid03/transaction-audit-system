import pandas as pd

from transaction_audit.api import analyse_data, inspect_data


def test_approval_cases_through_upload_mapping():
    rows = [
        ("19800", "50000", "yes", "GBP", None),
        ("27600", "5000", "no", "GBP", "22,600.00"),
        ("51000", "50000", "no", "GBP", "1,000.00"),
        ("49999.99", "50000", "yes", "GBP", None),
        ("50000", "50000", "yes", "GBP", None),
        ("50000.01", "50000", "yes", "GBP", "0.01"),
        ("5100", "5000", "no", "GBP", "100.00"),
        ("5100", "", "no", "GBP", "gap"),
        ("5100", "5000", "no", "USD", "gap"),
    ]
    frame = pd.DataFrame(rows, columns=["amount", "approval_limit_gbp", "approval_policy_met", "currency", "expected"])
    expected = frame.pop("expected")
    frame = frame.assign(transaction_id=[str(i) for i in frame.index], counterparty=[f"Supplier {i}" for i in frame.index],
                         account_id="AP", transaction_date="2025-01-01")
    mapping = inspect_data(frame, "review.csv")["mapping"]
    run = analyse_data(frame, "review.csv", mapping)
    for record, excess in zip(run["transactions"], expected, strict=True):
        findings = {f["technical_issue_type"]: f for f in record["findings"]}
        assert not record["model"]["flagged"]
        if excess == "gap":
            assert "approval_evidence_gap" in findings
        elif pd.isna(excess):
            assert not findings
        else:
            assert excess in findings["approval_authority_exceeded"]["evidence"]
            assert record["audit_band"] == "high"
            assert any(record["id"] in [m["id"] for m in case["members"]] for case in run["review_cases"])
    assert any(f["technical_issue_type"] == "approval_status_discrepancy" for f in run["transactions"][5]["findings"])


def test_normalised_invoice_settlement_uses_all_linked_payments():
    frame = pd.DataFrame({"transaction_id": ["T000033", "T000034"], "account_id": "AP",
                          "transaction_date": ["2025-01-26", "2025-01-30"], "currency": "GBP",
                          "counterparty": "Harbour Logistics 04 Limited", "vendor_id": "V0004",
                          "amount": [2060, 2036], "invoice_amount": 2060,
                          "invoice_reference": ["INV-001202", "inv 001202"]})
    run = analyse_data(frame, "fuz.csv", inspect_data(frame, "fuz.csv")["mapping"])
    case, = run["review_cases"]
    assert case["invoice_summary"]["paid_percent"] == "198.83"
    assert float(case["invoice_summary"]["excess"]) == 2036
    assert len(case["members"]) == 2
