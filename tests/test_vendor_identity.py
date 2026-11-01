import pandas as pd
import pytest

from transaction_audit.validation import validate_transactions
from transaction_audit.workflow import build_mapping_plan, suggested_mapping_columns


@pytest.mark.parametrize("invoice_total,issue", [(100, "possible_duplicate_payment"), (200, "possible_split_payment"), (None, "possible_duplicate_payment")])
@pytest.mark.parametrize("ids,names,expected", [
    (["V1", "V1"], ["Acme Ltd", "Acme Limited"], True),
    (["V1", "V2"], ["Acme", "Acme"], False),
    ([None, " "], [" ACME ", "Acme"], True),
    (["V1", None], ["Acme", "Acme"], False),
    (["Acme", None], ["Other", "Acme"], False),
])
def test_payment_grouping_respects_vendor_identity(invoice_total, issue, ids, names, expected):
    frame = pd.DataFrame({
        "transaction_id": ["T1", "T2"],
        "transaction_date": ["2026-01-01", "2026-01-02"],
        "account_id": ["A", "A"], "amount": [100, 100],
        "currency": ["GBP", "GBP"], "counterparty": names,
        "vendor_id": ids, "invoice_reference": ["INV-01", "INV-01"],
    })
    if invoice_total is not None:
        frame["invoice_amount"] = invoice_total
    original = frame.copy(deep=True)
    result = validate_transactions(frame)
    matches = [finding for finding in result.issues if finding.issue_type == issue]
    assert len(matches) == (2 if expected else 0)
    pd.testing.assert_frame_equal(frame, original)
    if expected:
        assert {finding.related_record_id for finding in matches} == {"T1", "T2"}


def test_enriched_ap_mapping_excludes_metadata():
    frame = pd.DataFrame(columns=[
        "vendor_name", "vendor_id", "amount", "invoice_amount", "credit_note_amount",
        "vendor_typical_amount", "vendor_risk", "vendor_country", "vendor_status",
        "vendor_invoice_zero_padding_rule",
    ])
    assert build_mapping_plan(frame).canonical_to_source["counterparty"] == "vendor_name"
    suggestions = suggested_mapping_columns(frame)
    assert suggestions["counterparty"] == ["vendor_name"]
    assert suggestions["amount"] == ["amount"]
    assert suggestions["vendor_id"] == ["vendor_id"]
