from __future__ import annotations

import csv
from dataclasses import asdict
from functools import lru_cache
import hashlib
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import stat
from typing import Any, Mapping

import pandas as pd
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from forecasting._interval_policy_candidate_revision_common import (
    canonical, digest, utc_timestamp,
)
from forecasting._interval_policy_retained_compatibility_common import (
    COMPATIBILITY_CONTRACT_VERSION,
    COMPATIBILITY_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityError,
    compatibility_policy_candidates,
    compatibility_summary_sha256,
    prepare_compatibility_summary,
)

ARTIFACT_ROLES = ("slices", "summary", "report")
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "data-contracts" / (
    "interval_policy_retained_compatibility_manifest_schema.json"
)


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


@lru_cache(maxsize=1)
def _manifest_validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _artifact_name(value: Any, role: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or value in {".", ".."}
        or any(character in value for character in ("/", "\\", ":"))
        or any(ord(character) < 32 for character in value)
        or Path(value).name != value
    ):
        raise IntervalPolicyRetainedCompatibilityError(
            "Manifest artifact filenames must be exact, safe basenames."
        )
    allowed = {".md"} if role == "report" else {".csv", ".parquet", ".pq"}
    if Path(value).suffix.casefold() not in allowed:
        raise IntervalPolicyRetainedCompatibilityError(
            f"Manifest {role} artifact has an unsupported file format."
        )
    return value


def _snapshot(path: Path, *, retain_bytes: bool = False) -> tuple[str, bytes | None]:
    """Hash a regular file; retain the summary's very same bytes for parsing.

    This guards leaf symlinks and detectable replacement during a read, not
    concurrent hostile mutation of the caller-controlled directory hierarchy.
    """
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact must be a regular non-symlink file: {path.name}."
            )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        hasher = hashlib.sha256()
        chunks: list[bytes] = []
        byte_count = 0
        with os.fdopen(descriptor, "rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (
                opened.st_dev, opened.st_ino
            ) != (before.st_dev, before.st_ino):
                raise IntervalPolicyRetainedCompatibilityError(
                    f"Compatibility artifact changed while opening: {path.name}."
                )
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
                byte_count += len(chunk)
                if retain_bytes:
                    chunks.append(chunk)
            after = os.fstat(handle.fileno())
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(opened, field) != getattr(after, field) for field in fields):
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact changed while reading: {path.name}."
            )
        if byte_count == 0:
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact must not be empty: {path.name}."
            )
        return hasher.hexdigest(), b"".join(chunks) if retain_bytes else None
    except OSError as exc:
        raise IntervalPolicyRetainedCompatibilityError(
            f"Cannot safely read compatibility artifact {path.name}."
        ) from exc


def _verify_saved_summary(content: bytes, filename: str, prepared: pd.DataFrame) -> None:
    try:
        if Path(filename).suffix.casefold() == ".csv":
            header = next(csv.reader(StringIO(content.decode("utf-8-sig"))))
            if len(header) != len(set(header)):
                raise ValueError("Duplicate saved summary columns are not allowed.")
            # Preserve identifiers such as '0012' or 'NA' rather than infer numbers/nulls.
            text_types = {
                column: str for column in prepared.select_dtypes(include=["object", "string"]).columns
            }
            saved = pd.read_csv(
                BytesIO(content), dtype=text_types,
                keep_default_na=False, float_precision="round_trip",
            )
        else:
            saved = pd.read_parquet(BytesIO(content))
        saved_digest = compatibility_summary_sha256(saved)
    except (ValueError, TypeError, KeyError, OverflowError, StopIteration, OSError) as exc:
        raise IntervalPolicyRetainedCompatibilityError(
            f"Saved compatibility summary is invalid: {exc}"
        ) from exc
    if saved_digest != compatibility_summary_sha256(prepared):
        raise IntervalPolicyRetainedCompatibilityError(
            "Saved compatibility summary digest differs from the supplied summary."
        )


def build_compatibility_manifest(
    summary: pd.DataFrame, *, artifacts: Mapping[str, Path]
) -> dict[str, Any]:
    prepared = prepare_compatibility_summary(summary)
    if set(artifacts) != set(ARTIFACT_ROLES):
        raise IntervalPolicyRetainedCompatibilityError(
            "Manifest requires slices, summary, and report artifacts."
        )
    paths = {role: Path(artifacts[role]) for role in ARTIFACT_ROLES}
    try:
        directories = {path.parent.resolve(strict=True) for path in paths.values()}
    except OSError as exc:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility artifact directory does not exist."
        ) from exc
    if len(directories) != 1:
        raise IntervalPolicyRetainedCompatibilityError(
            "Compatibility artifacts must share one artifact directory."
        )
    artifact_rows = []
    for role, path in paths.items():
        name = _artifact_name(path.name, role)
        artifact_hash, _ = _snapshot(path)
        artifact_rows.append({"role": role, "filename": name, "sha256": artifact_hash})
    previous, current = compatibility_policy_candidates()
    document: dict[str, Any] = {
        "compatibility_run_id": prepared["compatibility_run_id"].iloc[0],
        "compatibility_run_timestamp_utc": prepared["compatibility_run_timestamp_utc"].iloc[0],
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "compatibility_summary_sha256": compatibility_summary_sha256(prepared),
        "previous_policy": canonical(asdict(previous)),
        "current_policy": canonical(asdict(current)),
        "artifacts": artifact_rows,
        "compatibility_contract_version": COMPATIBILITY_CONTRACT_VERSION,
        **{field: False for field in COMPATIBILITY_SAFETY_FIELDS},
    }
    document["manifest_sha256"] = digest(document)
    document = canonical(document)
    # Construction must not bless a different saved summary or weaker artifact contract.
    verify_compatibility_manifest(document, prepared, artifact_directory=directories.pop())
    return document


def verify_compatibility_manifest(
    manifest: Mapping[str, Any],
    summary: pd.DataFrame,
    *,
    artifact_directory: Path,
) -> None:
    """Verify v2 structure, policy and saved-summary bindings without writes.

    Hashes detect inconsistent supplied evidence, not trusted authorship. This
    is not an independent replay of the original monitoring calculations.
    """
    if not isinstance(manifest, Mapping):
        raise IntervalPolicyRetainedCompatibilityError("Compatibility manifest must be an object.")
    document = dict(manifest)
    prepared = prepare_compatibility_summary(summary)
    try:
        _manifest_validator().validate(document)
    except ValidationError as exc:
        raise IntervalPolicyRetainedCompatibilityError(
            f"Compatibility manifest schema is invalid: {exc.message}"
        ) from exc
    expected = digest({key: value for key, value in document.items() if key != "manifest_sha256"})
    if document["manifest_sha256"] != expected:
        raise IntervalPolicyRetainedCompatibilityError("Compatibility manifest hash is invalid.")
    for field, candidate in zip(("previous_policy", "current_policy"), compatibility_policy_candidates()):
        if digest(document[field]) != digest(asdict(candidate)):
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility manifest {field} must exactly match its canonical snapshot."
            )
    for field in ("compatibility_run_id", "trend_run_id"):
        if document[field] != prepared[field].iloc[0]:
            raise IntervalPolicyRetainedCompatibilityError(f"Manifest and summary {field} differ.")
    try:
        timestamp = utc_timestamp(document["compatibility_run_timestamp_utc"], "compatibility_run_timestamp_utc")
    except (ValueError, TypeError) as exc:
        raise IntervalPolicyRetainedCompatibilityError("Manifest timestamp must be timezone-aware.") from exc
    if timestamp != prepared["compatibility_run_timestamp_utc"].iloc[0]:
        raise IntervalPolicyRetainedCompatibilityError("Manifest and summary assessment timestamps differ.")
    if document["compatibility_summary_sha256"] != compatibility_summary_sha256(prepared):
        raise IntervalPolicyRetainedCompatibilityError("Compatibility manifest summary digest is invalid.")
    artifacts = document["artifacts"]
    if len(artifacts) != len(ARTIFACT_ROLES) or {item["role"] for item in artifacts} != set(ARTIFACT_ROLES):
        raise IntervalPolicyRetainedCompatibilityError("Manifest must contain exactly one artifact per role.")
    names = [_artifact_name(item["filename"], item["role"]) for item in artifacts]
    if len({name.casefold() for name in names}) != len(names):
        raise IntervalPolicyRetainedCompatibilityError("Manifest artifact filenames must be distinct.")
    directory = Path(artifact_directory)
    for item in artifacts:
        is_summary = item["role"] == "summary"
        actual, content = _snapshot(directory / item["filename"], retain_bytes=is_summary)
        if actual != item["sha256"]:
            raise IntervalPolicyRetainedCompatibilityError(
                f"Compatibility artifact hash is invalid for {item['filename']}."
            )
        if is_summary:
            assert content is not None
            _verify_saved_summary(content, item["filename"], prepared)


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
