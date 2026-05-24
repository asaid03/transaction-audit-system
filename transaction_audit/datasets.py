from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pandas as pd


class DatasetType(str, Enum):
    TRANSACTIONS = "transactions"
    VENDORS = "vendors"
    BENEFICIARIES = "beneficiaries"
    BUDGET = "budget"


@dataclass(frozen=True)
class DatasetDefinition:
    dataset_type: DatasetType
    display_name: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()


DATASET_DEFINITIONS = {
    DatasetType.TRANSACTIONS: DatasetDefinition(
        dataset_type=DatasetType.TRANSACTIONS,
        display_name="Transactions",
        required_columns=("transaction_id", "transaction_date", "amount", "currency"),
        optional_columns=(
            "counterparty",
            "vendor_id",
            "beneficiary_id",
            "budget_line_id",
            "account_id",
            "budget_category",
            "description",
        ),
    ),
    DatasetType.VENDORS: DatasetDefinition(
        dataset_type=DatasetType.VENDORS,
        display_name="Vendors",
        required_columns=("vendor_id", "vendor_name"),
        optional_columns=("status", "contact_phone", "tax_id", "created_date"),
    ),
    DatasetType.BENEFICIARIES: DatasetDefinition(
        dataset_type=DatasetType.BENEFICIARIES,
        display_name="Beneficiaries",
        required_columns=("beneficiary_id", "beneficiary_name"),
        optional_columns=("expected_amount", "phone", "payment_account", "status"),
    ),
    DatasetType.BUDGET: DatasetDefinition(
        dataset_type=DatasetType.BUDGET,
        display_name="Budget",
        required_columns=("budget_line_id", "budget_name", "approved_amount", "currency"),
        optional_columns=("category", "period_start", "period_end", "donor"),
    ),
}


@dataclass(frozen=True)
class AuditDataset:
    dataset_type: DatasetType
    dataframe: pd.DataFrame
    source_name: str

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))


@dataclass(frozen=True)
class AuditBundle:
    transactions: AuditDataset
    vendors: AuditDataset | None = None
    beneficiaries: AuditDataset | None = None
    budget: AuditDataset | None = None

    def reference_datasets(self) -> list[AuditDataset]:
        return [
            dataset
            for dataset in [self.vendors, self.beneficiaries, self.budget]
            if dataset is not None
        ]

    def has_reference_data(self) -> bool:
        return bool(self.reference_datasets())

    def dataset_summary(self) -> pd.DataFrame:
        rows = []
        for dataset_type in DatasetType:
            dataset = self.get(dataset_type)
            rows.append(
                {
                    "dataset": dataset_type.value,
                    "uploaded": dataset is not None,
                    "source_name": dataset.source_name if dataset else None,
                    "rows": dataset.row_count if dataset else 0,
                }
            )
        return pd.DataFrame(rows, columns=["dataset", "uploaded", "source_name", "rows"])

    def get(self, dataset_type: DatasetType) -> AuditDataset | None:
        if dataset_type == DatasetType.TRANSACTIONS:
            return self.transactions
        if dataset_type == DatasetType.VENDORS:
            return self.vendors
        if dataset_type == DatasetType.BENEFICIARIES:
            return self.beneficiaries
        if dataset_type == DatasetType.BUDGET:
            return self.budget
        raise ValueError(f"Unsupported dataset type: {dataset_type}")


def required_columns_for_dataset(dataset_type: DatasetType) -> tuple[str, ...]:
    return DATASET_DEFINITIONS[dataset_type].required_columns


def optional_columns_for_dataset(dataset_type: DatasetType) -> tuple[str, ...]:
    return DATASET_DEFINITIONS[dataset_type].optional_columns
