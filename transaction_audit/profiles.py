from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from transaction_audit.config import REQUIRED_COLUMNS
from transaction_audit.datasets import DatasetType, optional_columns_for_dataset, required_columns_for_dataset


PROFILE_SCHEMA_VERSION = 2
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROFILES_DIR = PROJECT_ROOT / "import_profiles"


@dataclass(frozen=True)
class ImportProfile:
    profile_id: str
    display_name: str
    source_to_canonical: dict[str, str]
    dataset_type: str = DatasetType.TRANSACTIONS.value
    schema_version: int = PROFILE_SCHEMA_VERSION
    description: str = ""
    created_at_utc: str = ""
    updated_at_utc: str = ""

    @property
    def complete_required_fields(self) -> bool:
        required = set(required_profile_fields(self.dataset_type))
        return required.issubset(set(self.source_to_canonical.values()))

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def profile_id_from_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower()).strip("-")
    if not cleaned:
        raise ValueError("Profile name must contain at least one letter or number.")
    return cleaned


def list_import_profiles(
    profiles_dir: Path = DEFAULT_PROFILES_DIR,
    dataset_type: DatasetType | str | None = None,
) -> list[ImportProfile]:
    if not profiles_dir.exists():
        return []

    profiles = []
    for path in sorted(profiles_dir.glob("*.json")):
        profile = load_import_profile(path.stem, profiles_dir=profiles_dir)
        if dataset_type is None or profile.dataset_type == normalize_dataset_type(dataset_type).value:
            profiles.append(profile)
    return profiles


def profiles_dataframe(profiles: list[ImportProfile]) -> "pd.DataFrame":
    import pandas as pd

    rows = [
        {
            "profile_id": profile.profile_id,
            "dataset_type": profile.dataset_type,
            "display_name": profile.display_name,
            "description": profile.description,
            "complete_required_fields": profile.complete_required_fields,
            "created_at_utc": profile.created_at_utc,
            "updated_at_utc": profile.updated_at_utc,
        }
        for profile in profiles
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "profile_id",
            "dataset_type",
            "display_name",
            "description",
            "complete_required_fields",
            "created_at_utc",
            "updated_at_utc",
        ],
    )


def load_import_profile(
    profile_id: str,
    profiles_dir: Path = DEFAULT_PROFILES_DIR,
) -> ImportProfile:
    path = profiles_dir / f"{profile_id}.json"
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    return ImportProfile(
        profile_id=str(data["profile_id"]),
        display_name=str(data["display_name"]),
        source_to_canonical=dict(data["source_to_canonical"]),
        dataset_type=str(data.get("dataset_type", DatasetType.TRANSACTIONS.value)),
        schema_version=int(data.get("schema_version", PROFILE_SCHEMA_VERSION)),
        description=str(data.get("description", "")),
        created_at_utc=str(data.get("created_at_utc", "")),
        updated_at_utc=str(data.get("updated_at_utc", "")),
    )


def save_import_profile(
    display_name: str,
    source_to_canonical: dict[str, str],
    description: str = "",
    profiles_dir: Path = DEFAULT_PROFILES_DIR,
    dataset_type: DatasetType | str = DatasetType.TRANSACTIONS,
) -> ImportProfile:
    dataset = normalize_dataset_type(dataset_type)
    validate_profile_mapping(source_to_canonical, dataset_type=dataset)
    profiles_dir.mkdir(parents=True, exist_ok=True)

    profile_id = profile_id_for_dataset(display_name, dataset)
    path = profiles_dir / f"{profile_id}.json"
    now = datetime.now(UTC).isoformat()
    created_at = now

    if path.exists():
        existing = load_import_profile(profile_id, profiles_dir=profiles_dir)
        created_at = existing.created_at_utc or now

    profile = ImportProfile(
        profile_id=profile_id,
        display_name=display_name.strip(),
        source_to_canonical=dict(source_to_canonical),
        dataset_type=dataset.value,
        description=description.strip(),
        created_at_utc=created_at,
        updated_at_utc=now,
    )

    with path.open("w", encoding="utf-8") as handle:
        json.dump(profile.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")

    return profile


def delete_import_profile(
    profile_id: str,
    profiles_dir: Path = DEFAULT_PROFILES_DIR,
) -> bool:
    path = profiles_dir / f"{profile_id_from_name(profile_id)}.json"
    if not path.exists():
        return False

    path.unlink()
    return True


def validate_profile_mapping(
    source_to_canonical: dict[str, str],
    dataset_type: DatasetType | str = DatasetType.TRANSACTIONS,
) -> None:
    dataset = normalize_dataset_type(dataset_type)
    required_fields = set(required_profile_fields(dataset))
    allowed_fields = set(allowed_profile_fields(dataset))
    invalid_fields = sorted(
        canonical for canonical in set(source_to_canonical.values()) if canonical not in allowed_fields
    )
    if invalid_fields:
        joined = ", ".join(invalid_fields)
        raise ValueError(f"Profile contains unsupported canonical fields: {joined}")

    target_counts = Counter(source_to_canonical.values())
    duplicate_targets = sorted(field for field, count in target_counts.items() if count > 1)
    if duplicate_targets:
        joined = ", ".join(duplicate_targets)
        raise ValueError(f"Profile maps more than one source column to the same field: {joined}")

    missing_required = sorted(required_fields - set(source_to_canonical.values()))
    if missing_required:
        joined = ", ".join(missing_required)
        raise ValueError(f"Profile is missing required fields: {joined}")


def profile_id_for_dataset(display_name: str, dataset_type: DatasetType | str) -> str:
    dataset = normalize_dataset_type(dataset_type)
    base_id = profile_id_from_name(display_name)
    if dataset == DatasetType.TRANSACTIONS:
        return base_id
    return f"{dataset.value}-{base_id}"


def normalize_dataset_type(dataset_type: DatasetType | str) -> DatasetType:
    if isinstance(dataset_type, DatasetType):
        return dataset_type
    return DatasetType(str(dataset_type))


def required_profile_fields(dataset_type: DatasetType | str) -> tuple[str, ...]:
    dataset = normalize_dataset_type(dataset_type)
    if dataset == DatasetType.TRANSACTIONS:
        return tuple(REQUIRED_COLUMNS)
    return required_columns_for_dataset(dataset)


def allowed_profile_fields(dataset_type: DatasetType | str) -> tuple[str, ...]:
    dataset = normalize_dataset_type(dataset_type)
    if dataset == DatasetType.TRANSACTIONS:
        return tuple(dict.fromkeys([*REQUIRED_COLUMNS, *optional_columns_for_dataset(dataset)]))
    return tuple(dict.fromkeys([*required_columns_for_dataset(dataset), *optional_columns_for_dataset(dataset)]))
