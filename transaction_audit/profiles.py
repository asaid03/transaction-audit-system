from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from transaction_audit.config import REQUIRED_COLUMNS


PROFILE_SCHEMA_VERSION = 1
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROFILES_DIR = PROJECT_ROOT / "import_profiles"


@dataclass(frozen=True)
class ImportProfile:
    profile_id: str
    display_name: str
    source_to_canonical: dict[str, str]
    schema_version: int = PROFILE_SCHEMA_VERSION
    description: str = ""
    created_at_utc: str = ""
    updated_at_utc: str = ""

    @property
    def complete_required_fields(self) -> bool:
        return set(REQUIRED_COLUMNS).issubset(set(self.source_to_canonical.values()))

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def profile_id_from_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower()).strip("-")
    if not cleaned:
        raise ValueError("Profile name must contain at least one letter or number.")
    return cleaned


def list_import_profiles(profiles_dir: Path = DEFAULT_PROFILES_DIR) -> list[ImportProfile]:
    if not profiles_dir.exists():
        return []

    profiles = []
    for path in sorted(profiles_dir.glob("*.json")):
        profiles.append(load_import_profile(path.stem, profiles_dir=profiles_dir))
    return profiles


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
) -> ImportProfile:
    validate_profile_mapping(source_to_canonical)
    profiles_dir.mkdir(parents=True, exist_ok=True)

    profile_id = profile_id_from_name(display_name)
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
        description=description.strip(),
        created_at_utc=created_at,
        updated_at_utc=now,
    )

    with path.open("w", encoding="utf-8") as handle:
        json.dump(profile.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")

    return profile


def validate_profile_mapping(source_to_canonical: dict[str, str]) -> None:
    invalid_fields = sorted(
        canonical for canonical in set(source_to_canonical.values()) if canonical not in REQUIRED_COLUMNS
    )
    if invalid_fields:
        joined = ", ".join(invalid_fields)
        raise ValueError(f"Profile contains unsupported canonical fields: {joined}")

    target_counts = Counter(source_to_canonical.values())
    duplicate_targets = sorted(field for field, count in target_counts.items() if count > 1)
    if duplicate_targets:
        joined = ", ".join(duplicate_targets)
        raise ValueError(f"Profile maps more than one source column to the same field: {joined}")

    missing_required = sorted(set(REQUIRED_COLUMNS) - set(source_to_canonical.values()))
    if missing_required:
        joined = ", ".join(missing_required)
        raise ValueError(f"Profile is missing required fields: {joined}")
