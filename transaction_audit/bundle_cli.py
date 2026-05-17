from __future__ import annotations

import argparse
from pathlib import Path

from transaction_audit.datasets import AuditBundle, AuditDataset, DatasetType
from transaction_audit.ingestion import load_transactions
from transaction_audit.reconciliation import reconcile_bundle


def build_bundle_from_paths(
    transactions_path: Path,
    vendors_path: Path | None = None,
    beneficiaries_path: Path | None = None,
) -> AuditBundle:
    return AuditBundle(
        transactions=_load_dataset(DatasetType.TRANSACTIONS, transactions_path),
        vendors=_load_optional_dataset(DatasetType.VENDORS, vendors_path),
        beneficiaries=_load_optional_dataset(DatasetType.BENEFICIARIES, beneficiaries_path),
    )


def _load_optional_dataset(dataset_type: DatasetType, path: Path | None) -> AuditDataset | None:
    if path is None:
        return None
    return _load_dataset(dataset_type, path)


def _load_dataset(dataset_type: DatasetType, path: Path) -> AuditDataset:
    return AuditDataset(
        dataset_type=dataset_type,
        dataframe=load_transactions(path),
        source_name=path.name,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 2 multi-file reconciliation checks.")
    parser.add_argument("--transactions", required=True, type=Path)
    parser.add_argument("--vendors", type=Path)
    parser.add_argument("--beneficiaries", type=Path)
    args = parser.parse_args()

    bundle = build_bundle_from_paths(
        transactions_path=args.transactions,
        vendors_path=args.vendors,
        beneficiaries_path=args.beneficiaries,
    )
    result = reconcile_bundle(bundle)
    issues = result.issues_dataframe()
    skipped = result.skipped_checks_dataframe()

    print("Dataset summary")
    print(bundle.dataset_summary().to_string(index=False))
    print()
    print(f"Reference issues found: {len(issues)}")
    if not issues.empty:
        print(issues.to_string(index=False))
    print()
    print(f"Skipped checks: {len(skipped)}")
    if not skipped.empty:
        print(skipped.to_string(index=False))


if __name__ == "__main__":
    main()
