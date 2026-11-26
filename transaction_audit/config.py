from dataclasses import dataclass
import math
import re
import sys

import pandas as pd


REQUIRED_COLUMNS = [
    "transaction_id",
    "transaction_date",
    "account_id",
    "amount",
    "currency",
    "counterparty",
]


DEFAULT_COLUMN_ALIASES = {
    "transaction_id": "transaction_id",
    "transaction id": "transaction_id",
    "txn_id": "transaction_id",
    "txn id": "transaction_id",
    "txn_ref": "transaction_id",
    "txn ref": "transaction_id",
    "transaction_ref": "transaction_id",
    "transaction ref": "transaction_id",
    "id": "transaction_id",
    "reference": "transaction_id",
    "ref": "transaction_id",
    "transaction_date": "transaction_date",
    "transaction date": "transaction_date",
    "txn_date": "transaction_date",
    "txn date": "transaction_date",
    "date": "transaction_date",
    "account_id": "account_id",
    "account id": "account_id",
    "acc_id": "account_id",
    "acc id": "account_id",
    "acct": "account_id",
    "acct_id": "account_id",
    "acct id": "account_id",
    "account": "account_id",
    "account_code": "account_id",
    "account code": "account_id",
    "account_number": "account_id",
    "account number": "account_id",
    "nominal_code": "account_id",
    "nominal code": "account_id",
    "gl_code": "account_id",
    "gl code": "account_id",
    "amount": "amount",
    "amt": "amount",
    "value": "amount",
    "currency": "currency",
    "ccy": "currency",
    "counterparty": "counterparty",
    "counterparty_name": "counterparty",
    "counterparty name": "counterparty",
    "vendor": "counterparty",
    "vendor_name": "counterparty",
    "vendor name": "counterparty",
    "supplier_name": "counterparty",
    "supplier name": "counterparty",
    "merchant": "counterparty",
    "payee": "counterparty",
    "invoice_reference": "invoice_reference",
    "invoice reference": "invoice_reference",
    "invoice_ref": "invoice_reference",
    "invoice ref": "invoice_reference",
    "invoice": "invoice_reference",
    "invoice_no": "invoice_reference",
    "invoice no": "invoice_reference",
    "invoice_number": "invoice_reference",
    "invoice number": "invoice_reference",
    "document_no": "invoice_reference",
    "document no": "invoice_reference",
    "document_number": "invoice_reference",
    "document number": "invoice_reference",
    "approval_limit_gbp": "approval_limit_gbp",
    "approval limit gbp": "approval_limit_gbp",
    "approval_policy_met": "approval_policy_met",
    "approval policy met": "approval_policy_met",
    "invoice_amount": "invoice_amount",
    "invoice amount": "invoice_amount",
    "invoice_total": "invoice_amount",
    "invoice total": "invoice_amount",
    "invoice_value": "invoice_amount",
    "invoice value": "invoice_amount",
    "vendor_id": "vendor_id",
    "vendor id": "vendor_id",
    "vendor_code": "vendor_id",
    "vendor code": "vendor_id",
    "supplier_id": "vendor_id",
    "supplier id": "vendor_id",
    "supplier_code": "vendor_id",
    "supplier code": "vendor_id",
    "beneficiary_id": "beneficiary_id",
    "beneficiary id": "beneficiary_id",
    "beneficiary_code": "beneficiary_id",
    "beneficiary code": "beneficiary_id",
    "recipient_id": "beneficiary_id",
    "recipient id": "beneficiary_id",
    "recipient_code": "beneficiary_id",
    "recipient code": "beneficiary_id",
    "budget_line_id": "budget_line_id",
    "budget line id": "budget_line_id",
    "budget_code": "budget_line_id",
    "budget code": "budget_line_id",
    "budget_category": "budget_category",
    "budget category": "budget_category",
    "category": "budget_category",
    "spend_category": "budget_category",
    "spend category": "budget_category",
    "balance": "balance",
    "running balance": "balance",
    "period": "period",
    "reporting period": "period",
    "payment period": "period",
    "programme_id": "programme_id",
    "programme id": "programme_id",
    "programme_code": "programme_id",
    "programme code": "programme_id",
    "program_id": "programme_id",
    "program id": "programme_id",
    "program_code": "programme_id",
    "program code": "programme_id",
    "project_id": "project_id",
    "project id": "project_id",
    "project_code": "project_id",
    "project code": "project_id",
    "donor": "donor",
    "donor_name": "donor",
    "donor name": "donor",
    "location": "location",
    "site": "location",
}

VENDOR_COLUMN_ALIASES = {
    "vendor_id": "vendor_id",
    "vendor id": "vendor_id",
    "vendor_code": "vendor_id",
    "vendor code": "vendor_id",
    "supplier_id": "vendor_id",
    "supplier id": "vendor_id",
    "supplier_code": "vendor_id",
    "supplier code": "vendor_id",
    "id": "vendor_id",
    "vendor_name": "vendor_name",
    "vendor name": "vendor_name",
    "supplier_name": "vendor_name",
    "supplier name": "vendor_name",
    "vendor": "vendor_name",
    "supplier": "vendor_name",
    "name": "vendor_name",
}

BENEFICIARY_COLUMN_ALIASES = {
    "beneficiary_id": "beneficiary_id",
    "beneficiary id": "beneficiary_id",
    "beneficiary_code": "beneficiary_id",
    "beneficiary code": "beneficiary_id",
    "recipient_id": "beneficiary_id",
    "recipient id": "beneficiary_id",
    "recipient_code": "beneficiary_id",
    "recipient code": "beneficiary_id",
    "id": "beneficiary_id",
    "beneficiary_name": "beneficiary_name",
    "beneficiary name": "beneficiary_name",
    "recipient_name": "beneficiary_name",
    "recipient name": "beneficiary_name",
    "beneficiary": "beneficiary_name",
    "recipient": "beneficiary_name",
    "name": "beneficiary_name",
    "expected_amount": "expected_amount",
    "expected amount": "expected_amount",
    "entitlement_amount": "expected_amount",
    "entitlement amount": "expected_amount",
    "approved_amount": "expected_amount",
    "approved amount": "expected_amount",
    "currency": "currency",
    "ccy": "currency",
    "period": "period",
    "reporting period": "period",
    "payment period": "period",
    "programme_id": "programme_id",
    "programme id": "programme_id",
    "programme_code": "programme_id",
    "programme code": "programme_id",
    "project_id": "project_id",
    "project id": "project_id",
    "project_code": "project_id",
    "project code": "project_id",
    "donor": "donor",
    "location": "location",
    "site": "location",
}

BUDGET_COLUMN_ALIASES = {
    "budget_line_id": "budget_line_id",
    "budget line id": "budget_line_id",
    "budget_code": "budget_line_id",
    "budget code": "budget_line_id",
    "line_id": "budget_line_id",
    "line id": "budget_line_id",
    "budget_name": "budget_name",
    "budget name": "budget_name",
    "budget_line": "budget_name",
    "budget line": "budget_name",
    "line_name": "budget_name",
    "line name": "budget_name",
    "approved_amount": "approved_amount",
    "approved amount": "approved_amount",
    "budget_amount": "approved_amount",
    "budget amount": "approved_amount",
    "amount": "approved_amount",
    "currency": "currency",
    "ccy": "currency",
    "period": "period",
    "reporting period": "period",
    "budget period": "period",
    "programme_id": "programme_id",
    "programme id": "programme_id",
    "programme_code": "programme_id",
    "programme code": "programme_id",
    "project_id": "project_id",
    "project id": "project_id",
    "project_code": "project_id",
    "project code": "project_id",
    "donor": "donor",
    "donor_name": "donor",
    "donor name": "donor",
    "location": "location",
    "site": "location",
}

@dataclass(frozen=True)
class ReviewThreshold:
    currency: str
    amount: float

    def __post_init__(self):
        if not isinstance(self.currency, str) or not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("Enter a three-letter uppercase threshold currency, such as GBP.")
        if (
            isinstance(self.amount, bool)
            or not isinstance(self.amount, (int, float))
            or not 0 < self.amount <= sys.float_info.max
        ):
            raise ValueError("Enter a finite threshold amount greater than zero.")


DEMO_THRESHOLD = ReviewThreshold("GBP", 10_000)


@dataclass(frozen=True)
class ValidationConfig:
    review_threshold: ReviewThreshold | None = None
    invoice_amount_tolerance: float = 0.01
    allowed_currencies: tuple[str, ...] = ("GBP", "USD", "EUR")
    date_dayfirst: bool = True
    duplicate_payment_window_days: int = 2
    balance_tolerance: float = 0.01
    balance_inference_min_transitions: int = 5
    balance_inference_min_match_ratio: float = 0.80
    vendor_concentration_threshold: float = 0.40
    vendor_concentration_min_rows: int = 20
    vendor_concentration_min_counterparties: int = 3

    def threshold_settings(self) -> dict:
        threshold = self.review_threshold
        return {
            "threshold_amount": threshold.amount if threshold else None,
            "threshold_currency": threshold.currency if threshold else None,
            "threshold_basis": "Absolute individual transaction amount",
            "threshold_comparison": ">=",
        }

    def threshold_coverage(self, currencies: pd.Series) -> dict[str, float | None]:
        threshold = self.review_threshold
        return {
            str(currency): threshold.amount if threshold and currency == threshold.currency else None
            for currency in currencies.fillna("Unknown").unique()
        }

    def threshold_matches(self, amounts: pd.Series, currencies: pd.Series) -> pd.Series:
        threshold = self.review_threshold
        if threshold is None:
            return pd.Series(False, index=amounts.index)
        same_currency = currencies.astype("string").str.strip().str.upper().eq(threshold.currency)
        return (same_currency & amounts.abs().ge(threshold.amount) & amounts.abs().lt(math.inf)).fillna(False)
