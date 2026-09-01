from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import canonical, digest
from forecasting._interval_policy_retained_compatibility_common import (
    COMPATIBILITY_CONTRACT_VERSION,
    COMPATIBILITY_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityError,
    compatibility_policy_candidates,
    compatibility_summary_sha256,
    prepare_compatibility_summary,
)


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_compatibility_manifest(
    summary: pd.DataFrame, *, artifacts: Mapping[str, Path]
) -> dict[str, Any]:
    prepared = prepare_compatibility_summary(summary)
    if set(artifacts) != {"slices", "summary", "report"}:
        raise IntervalPolicyRetainedCompatibilityError(
            "Manifest requires slices, summary, and report artifacts."
        )
    artifact_rows = []
    for role in ("slices", "summary", "report"):
        path = Path(artifacts[role])
        if not path.is_file():
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact does not exist: {path}."
            )
        artifact_rows.append(
            {"role": role, "filename": path.name, "sha256": file_sha256(path)}
        )
    previous, current = compatibility_policy_candidates()
    document: dict[str, Any] = {
        "compatibility_run_id": prepared["compatibility_run_id"].iloc[0],
        "compatibility_run_timestamp_utc": prepared[
            "compatibility_run_timestamp_utc"
        ].iloc[0],
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "compatibility_summary_sha256": compatibility_summary_sha256(prepared),
        "previous_policy": canonical(asdict(previous)),
        "current_policy": canonical(asdict(current)),
        "artifacts": artifact_rows,
        "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
        **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
    }
    document["manifest_sha256"] = digest(document)
    return canonical(document)


def verify_compatibility_manifest(
    manifest: Mapping[str, Any],
    summary: pd.DataFrame,
    *,
    artifact_directory: Path,
) -> None:
    document = dict(manifest)
    prepared = prepare_compatibility_summary(summary)
    expected = digest(
        {key: value for key, value in document.items() if key != "manifest_sha256"}
    )
    if document.get("manifest_sha256") != expected:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility manifest hash is invalid."
        )
    if document.get("compatibility_contract_version") != COMPATIBILITY_CONTRACT_VERSION:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility manifest contract is invalid."
        )
    if document.get("compatibility_run_id") != prepared[
        "compatibility_run_id"
    ].iloc[0]:
        raise IntervalPolicyRetainedCompatibilityError(
            "Manifest and summary compatibility run IDs differ."
        )
    if document.get("trend_run_id") != prepared["trend_run_id"].iloc[0]:
        raise IntervalPolicyRetainedCompatibilityError(
            "Manifest and summary trend run IDs differ."
        )
    if document.get("compatibility_summary_sha256") != compatibility_summary_sha256(
        prepared
    ):
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility manifest summary digest is invalid."
        )
    for field in COMPATIBILITY_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility manifest safety field {field} must be false."
            )
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or {
        item.get("role") for item in artifacts if isinstance(item, Mapping)
    } != {"slices", "summary", "report"}:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility manifest artifacts are incomplete."
        )
    directory = Path(artifact_directory)
    for item in artifacts:
        if not isinstance(item, Mapping):
            raise IntervalPolicyRetainedCompatibilityError(
                "Manifest artifact entries must be objects."
            )
        filename = str(item.get("filename", "")).strip()
        if not filename or Path(filename).name != filename:
            raise IntervalPolicyRetainedCompatibilityError(
                "Manifest artifact filenames must be safe basenames."
            )
        path = directory / filename
        if not path.is_file() or file_sha256(path) != item.get("sha256"):
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact hash is invalid for {filename}."
            )


def write_json_atomic(document: Mapping[str, Any], path: Path) -> Path:
    path = Path(path)
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(
            json.dumps(canonical(document), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path
