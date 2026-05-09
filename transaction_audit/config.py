from dataclasses import dataclass


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
    "amount": "amount",
    "amt": "amount",
    "value": "amount",
    "currency": "currency",
    "ccy": "currency",
    "counterparty": "counterparty",
    "counterparty_name": "counterparty",
    "counterparty name": "counterparty",
    "vendor": "counterparty",
    "merchant": "counterparty",
    "payee": "counterparty",
}

@dataclass(frozen=True)
class ValidationConfig:
    large_amount_threshold: float = 10000.0
    allowed_currencies: tuple[str, ...] = ("GBP", "USD", "EUR")
    date_dayfirst: bool = True
