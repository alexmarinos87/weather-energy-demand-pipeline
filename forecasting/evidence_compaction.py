from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PLAN_CONTRACT_VERSION = "evidence-lifecycle-plan-v1"
SUMMARY_CONTRACT_VERSION = "evidence-lifecycle-summary-v1"
COMPACTION_MANIFEST_VERSION = "evidence-compaction-manifest-v1"
PLAN_ID_PATTERN = re.compile(r"^elp-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SUPPORTED_SUFFIXES = {".csv": "csv", ".parquet": "parquet", ".pq": "parquet"}

PLAN_REQUIRED_COLUMNS = {
    "plan_id",
    "plan_created_at_utc",
    "relative_path",
    "parent_path",
    "category",
    "suffix",
    "sha256",
    "size_bytes",
    "modified_at_utc",
    "protected_by_candidate",
    "always_protect",
    "planned_action",
    "requires_explicit_apply",
    "min_compaction_files",
    "max_compaction_source_bytes",
    "plan_contract_version",
}
SUMMARY_REQUIRED_FIELDS = {
    "plan_id",
    "plan_created_at_utc",
    "policy_contract_version",
    "policy_sha256",
    "mutation_performed",
    "summary_contract_version",
}


class EvidenceCompactionError(ValueError):
    """Raised when staged evidence compaction is unsafe or unverifiable."""


def _text(value: Any, name: str) -> str:
    if value is None:
        raise EvidenceCompactionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise EvidenceCompactionError(f"{name} must be non-empty.")
    return text


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    raise EvidenceCompactionError(f"{name} must be boolean.")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise EvidenceCompactionError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceCompactionError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise EvidenceCompactionError(f"{name} must be a non-negative integer.")
    return parsed


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceCompactionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise EvidenceCompactionError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


def _safe_relative(value: Any, name: str) -> Path:
    text = _text(value, name).replace("\\", "/")
    path = Path(text)
    if path.is_absolute() or text.startswith("/") or ".." in path.parts:
        raise EvidenceCompactionError(f"{name} must be a safe relative path.")
    if any(part in {"", "."} for part in path.parts):
        raise EvidenceCompactionError(f"{name} must be a normalized relative path.")
    return path


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_iso(value, "timestamp")
    if pd.isna(value):
        return None
    return value


def _digest(payload: dict[str, Any], *, exclude: tuple[str, ...] = ()) -> str:
    material = {key: value for key, value in payload.items() if key not in exclude}
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    material = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="ns",
        index=False,
        default_handler=str,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _schema(frame: pd.DataFrame) -> dict[str, Any]:
    if len(frame.columns) == 0:
        raise EvidenceCompactionError("Compaction sources must contain columns.")
    return {
        "columns": [str(column) for column in frame.columns],
        "dtypes": [str(dtype) for dtype in frame.dtypes],
    }


def _root(path: Path, name: str) -> Path:
    path = Path(path)
    if path.is_symlink():
        raise EvidenceCompactionError(f"{name} cannot be a symbolic link.")
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise EvidenceCompactionError(f"{name} must be an existing directory.")
    return resolved


def _reject_symlink_components(root: Path, relative: Path, name: str) -> None:
    current = root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise EvidenceCompactionError(
                f"{name} contains a symbolic-link component: {current}."
            )


def _resolved_under(
    root: Path,
    relative: Path,
    *,
    name: str,
    must_exist: bool,
) -> Path:
    _reject_symlink_components(root, relative, name)
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=must_exist)
    except FileNotFoundError as exc:
        raise EvidenceCompactionError(f"{name} does not exist: {relative}.") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvidenceCompactionError(f"{name} escapes the data root: {relative}.") from exc
    return resolved


def _recompute_plan_id(plan: pd.DataFrame, summary: dict[str, Any]) -> str:
    material = {
        "policy_contract_version": summary["policy_contract_version"],
        "policy_sha256": summary["policy_sha256"],
        "plan_created_at_utc": _utc_iso(
            summary["plan_created_at_utc"], "plan_created_at_utc"
        ),
        "rows": [
            {
                "relative_path": str(row.relative_path),
                "sha256": str(row.sha256),
                "planned_action": str(row.planned_action),
            }
            for row in plan.sort_values("relative_path").itertuples(index=False)
        ],
    }
    return "elp-" + _digest(material)[:24]


def prepare_compaction_groups(
    plan: pd.DataFrame,
    summary: dict[str, Any],
    *,
    confirm_plan_id: str,
) -> tuple[list[dict[str, Any]], str]:
    """Validate a lifecycle plan and return homogeneous compaction groups."""
    missing = sorted(PLAN_REQUIRED_COLUMNS - set(plan.columns))
    if missing:
        raise EvidenceCompactionError(
            "Lifecycle plan is missing required columns: " + ", ".join(missing) + "."
        )
    missing_summary = sorted(SUMMARY_REQUIRED_FIELDS - set(summary))
    if missing_summary:
        raise EvidenceCompactionError(
            "Lifecycle summary is missing required fields: "
            + ", ".join(missing_summary)
            + "."
        )
    if summary["summary_contract_version"] != SUMMARY_CONTRACT_VERSION:
        raise EvidenceCompactionError("Unsupported lifecycle summary contract.")
    if _boolean(summary["mutation_performed"], "mutation_performed"):
        raise EvidenceCompactionError(
            "Compaction can be staged only from a non-mutating lifecycle plan."
        )
    plan_ids = sorted({_text(value, "plan_id") for value in plan["plan_id"]})
    if len(plan_ids) != 1:
        raise EvidenceCompactionError("Lifecycle plan must contain exactly one plan_id.")
    plan_id = plan_ids[0]
    if not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise EvidenceCompactionError("Lifecycle plan_id is malformed.")
    if _text(summary["plan_id"], "summary.plan_id") != plan_id:
        raise EvidenceCompactionError("Lifecycle plan and summary IDs do not match.")
    if _text(confirm_plan_id, "confirm_plan_id") != plan_id:
        raise EvidenceCompactionError(
            "confirm_plan_id must exactly match the lifecycle plan."
        )
    if set(plan["plan_contract_version"].astype(str)) != {PLAN_CONTRACT_VERSION}:
        raise EvidenceCompactionError("Unsupported lifecycle plan contract.")
    policy_sha = _text(summary["policy_sha256"], "policy_sha256").lower()
    if not SHA256_PATTERN.fullmatch(policy_sha):
        raise EvidenceCompactionError("Lifecycle policy SHA-256 is malformed.")
    if _recompute_plan_id(plan, summary) != plan_id:
        raise EvidenceCompactionError(
            "Lifecycle plan identity does not match its content."
        )
    selected = plan.loc[plan["planned_action"] == "compact_candidate"].copy()
    if selected.empty:
        raise EvidenceCompactionError(
            "Lifecycle plan contains no compact_candidate rows."
        )
    normalized_paths: list[str] = []
    formats: list[str] = []
    for row in selected.itertuples(index=False):
        relative = _safe_relative(row.relative_path, "relative_path").as_posix()
        digest = _text(row.sha256, "sha256").lower()
        if not SHA256_PATTERN.fullmatch(digest):
            raise EvidenceCompactionError(
                f"Lifecycle plan contains a malformed SHA-256 for {relative}."
            )
        suffix = str(row.suffix).strip().lower()
        if suffix not in SUPPORTED_SUFFIXES:
            raise EvidenceCompactionError(
                f"Unsupported compaction source format for {relative}: {suffix}."
            )
        if _boolean(row.protected_by_candidate, "protected_by_candidate"):
            raise EvidenceCompactionError(
                f"Candidate-protected evidence cannot be compacted: {relative}."
            )
        if _boolean(row.always_protect, "always_protect"):
            raise EvidenceCompactionError(
                f"Always-protected evidence cannot be compacted: {relative}."
            )
        if not _boolean(row.requires_explicit_apply, "requires_explicit_apply"):
            raise EvidenceCompactionError(
                f"Compaction candidate lacks explicit-apply evidence: {relative}."
            )
        size = _non_negative_int(row.size_bytes, "size_bytes")
        maximum = _non_negative_int(
            row.max_compaction_source_bytes, "max_compaction_source_bytes"
        )
        if maximum < 1 or size > maximum:
            raise EvidenceCompactionError(
                f"Compaction source exceeds its planned byte bound: {relative}."
            )
        normalized_paths.append(relative)
        formats.append(SUPPORTED_SUFFIXES[suffix])
    if len(normalized_paths) != len(set(normalized_paths)):
        raise EvidenceCompactionError("Lifecycle plan contains duplicate source paths.")
    selected["relative_path"] = normalized_paths
    selected["source_format"] = formats
    selected["sha256"] = selected["sha256"].astype(str).str.lower()
    groups: list[dict[str, Any]] = []
    for key, group in selected.groupby(
        ["category", "parent_path", "source_format"],
        sort=True,
        dropna=False,
    ):
        thresholds = {
            _non_negative_int(value, "min_compaction_files")
            for value in group["min_compaction_files"]
        }
        if len(thresholds) != 1:
            raise EvidenceCompactionError(
                "Compaction group contains inconsistent file-count thresholds."
            )
        minimum = thresholds.pop()
        if minimum < 2 or len(group) < minimum:
            raise EvidenceCompactionError(
                f"Compaction group {key} contains {len(group)} files; minimum is {minimum}."
            )
        group_id = "cpg-" + _digest(
            {
                "plan_id": plan_id,
                "category": str(key[0]),
                "parent_path": str(key[1]),
                "source_format": str(key[2]),
                "paths": sorted(group["relative_path"].tolist()),
            }
        )[:20]
        groups.append(
            {
                "group_id": group_id,
                "category": _text(key[0], "category"),
                "parent_path": str(key[1]),
                "source_format": str(key[2]),
                "rows": group.sort_values("relative_path").reset_index(drop=True),
            }
        )
    return groups, plan_id


def _read_frame(path: Path, source_format: str) -> pd.DataFrame:
    if source_format == "csv":
        return pd.read_csv(path)
    if source_format == "parquet":
        return pd.read_parquet(path)
    raise EvidenceCompactionError(f"Unsupported source format: {source_format}.")


def _preflight_group(
    root: Path, group: dict[str, Any]
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, Any], str]:
    entries: list[dict[str, Any]] = []
    frames: list[pd.DataFrame] = []
    expected_schema: dict[str, Any] | None = None
    for sequence, row in enumerate(group["rows"].itertuples(index=False), start=1):
        relative = _safe_relative(row.relative_path, "relative_path")
        source = _resolved_under(root, relative, name="compaction source", must_exist=True)
        if source.is_symlink() or not source.is_file():
            raise EvidenceCompactionError(
                f"Compaction source must be a regular non-symlink file: {relative}."
            )
        size = _non_negative_int(row.size_bytes, "size_bytes")
        if source.stat().st_size != size:
            raise EvidenceCompactionError(
                f"Compaction source size changed after planning: {relative}."
            )
        digest = _sha256_file(source)
        if digest != row.sha256:
            raise EvidenceCompactionError(
                f"Compaction source content changed after planning: {relative}."
            )
        frame = _read_frame(source, group["source_format"])
        schema = _schema(frame)
        if expected_schema is None:
            expected_schema = schema
        elif schema != expected_schema:
            raise EvidenceCompactionError(
                f"Compaction source schema differs from the group: {relative}."
            )
        frames.append(frame)
        entries.append(
            {
                "entry_sequence": sequence,
                "source_relative_path": relative.as_posix(),
                "source_size_bytes": size,
                "source_sha256": digest,
                "source_row_count": int(len(frame)),
                "source_modified_at_utc": _utc_iso(
                    row.modified_at_utc, "modified_at_utc"
                ),
            }
        )
    assert expected_schema is not None
    combined = pd.concat(frames, ignore_index=True)
    return entries, combined, expected_schema, _frame_fingerprint(combined)


def _write_json_exclusive(payload: dict[str, Any], path: Path) -> Path:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite {path}.")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(_canonical(payload), handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def stage_compactions(
    data_root: Path,
    groups: list[dict[str, Any]],
    *,
    plan_id: str,
    actor: str,
    reason: str,
    staged_at_utc: Any | None = None,
    output_prefix: str = "compacted",
) -> list[tuple[dict[str, Any], Path]]:
    """Write verified compacted outputs while leaving every source untouched."""
    root = _root(data_root, "data_root")
    actor = _text(actor, "actor")
    reason = _text(reason, "reason")
    staged_at = _utc_iso(
        staged_at_utc or datetime.now(timezone.utc), "staged_at_utc"
    )
    prefix = _safe_relative(output_prefix, "output_prefix")
    preflight: list[dict[str, Any]] = []
    for group in groups:
        entries, combined, schema, fingerprint = _preflight_group(root, group)
        operation_material = {
            "plan_id": plan_id,
            "group_id": group["group_id"],
            "category": group["category"],
            "parent_path": group["parent_path"],
            "source_format": group["source_format"],
            "staged_at_utc": staged_at,
            "actor": actor,
            "reason": reason,
            "entries": entries,
            "combined_data_sha256": fingerprint,
        }
        operation_id = "cmp-" + _digest(operation_material)[:24]
        directory_relative = (
            prefix / plan_id / group["category"] / group["group_id"]
        )
        directory = _resolved_under(
            root, directory_relative, name="compaction output directory", must_exist=False
        )
        output_relative = directory_relative / f"compacted_{operation_id}.parquet"
        output_path = _resolved_under(
            root, output_relative, name="compacted output", must_exist=False
        )
        manifest_path = directory / f"compaction_manifest_{operation_id}.json"
        temporary_output = output_path.with_suffix(".tmp.parquet")
        for path in (output_path, manifest_path, temporary_output, manifest_path.with_suffix(".tmp")):
            if path.exists():
                raise FileExistsError(f"Compaction output already exists: {path}.")
        preflight.append(
            {
                **group,
                "entries": entries,
                "combined": combined,
                "schema": schema,
                "combined_data_sha256": fingerprint,
                "operation_id": operation_id,
                "output_relative": output_relative.as_posix(),
                "output_path": output_path,
                "temporary_output": temporary_output,
                "manifest_path": manifest_path,
            }
        )
    created: list[Path] = []
    results: list[tuple[dict[str, Any], Path]] = []
    try:
        for item in preflight:
            item["output_path"].parent.mkdir(parents=True, exist_ok=True)
            item["combined"].to_parquet(item["temporary_output"], index=False)
            item["temporary_output"].replace(item["output_path"])
            created.append(item["output_path"])
            verified = pd.read_parquet(item["output_path"])
            if _schema(verified) != item["schema"]:
                raise EvidenceCompactionError(
                    f"Compacted output schema verification failed for {item['group_id']}."
                )
            if len(verified) != len(item["combined"]):
                raise EvidenceCompactionError(
                    f"Compacted output row-count verification failed for {item['group_id']}."
                )
            verified_fingerprint = _frame_fingerprint(verified)
            if verified_fingerprint != item["combined_data_sha256"]:
                raise EvidenceCompactionError(
                    f"Compacted output data verification failed for {item['group_id']}."
                )
            output_size = item["output_path"].stat().st_size
            output_hash = _sha256_file(item["output_path"])
            manifest = {
                "operation_id": item["operation_id"],
                "plan_id": plan_id,
                "group_id": item["group_id"],
                "staged_at_utc": staged_at,
                "actor": actor,
                "reason": reason,
                "category": item["category"],
                "source_parent_path": item["parent_path"],
                "source_format": item["source_format"],
                "source_file_count": len(item["entries"]),
                "source_total_bytes": sum(
                    entry["source_size_bytes"] for entry in item["entries"]
                ),
                "source_total_rows": sum(
                    entry["source_row_count"] for entry in item["entries"]
                ),
                "source_schema": item["schema"],
                "source_entries": item["entries"],
                "compacted_output_relative_path": item["output_relative"],
                "compacted_output_size_bytes": output_size,
                "compacted_output_sha256": output_hash,
                "compacted_output_row_count": int(len(verified)),
                "compacted_output_data_sha256": verified_fingerprint,
                "output_verification_status": "verified",
                "source_files_mutated": False,
                "source_files_permanently_deleted": False,
                "replacement_authorized": False,
                "source_quarantine_required_before_replacement": True,
                "manifest_contract_version": COMPACTION_MANIFEST_VERSION,
            }
            manifest["manifest_hash"] = _digest(
                manifest, exclude=("manifest_hash",)
            )
            _write_json_exclusive(manifest, item["manifest_path"])
            created.append(item["manifest_path"])
            results.append((manifest, item["manifest_path"]))
    except Exception:
        for item in preflight:
            item["temporary_output"].unlink(missing_ok=True)
        for path in reversed(created):
            path.unlink(missing_ok=True)
        raise
    return results


def load_compaction_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if manifest.get("manifest_contract_version") != COMPACTION_MANIFEST_VERSION:
        raise EvidenceCompactionError("Unsupported compaction manifest contract.")
    if manifest.get("source_files_mutated") is not False:
        raise EvidenceCompactionError("Compaction manifest claims source mutation.")
    if manifest.get("source_files_permanently_deleted") is not False:
        raise EvidenceCompactionError("Compaction manifest claims source deletion.")
    if manifest.get("replacement_authorized") is not False:
        raise EvidenceCompactionError("Compaction manifest cannot authorize replacement.")
    if manifest.get("manifest_hash") != _digest(
        manifest, exclude=("manifest_hash",)
    ):
        raise EvidenceCompactionError("Compaction manifest hash is invalid.")
    entries = manifest.get("source_entries")
    if not isinstance(entries, list) or not entries:
        raise EvidenceCompactionError("Compaction manifest has no source entries.")
    if manifest.get("source_file_count") != len(entries):
        raise EvidenceCompactionError("Compaction source-file count is inconsistent.")
    return manifest


def verify_staged_compaction(
    data_root: Path, manifest: dict[str, Any]
) -> dict[str, Any]:
    """Verify source preservation and staged output equivalence."""
    root = _root(data_root, "data_root")
    frames: list[pd.DataFrame] = []
    total_bytes = 0
    total_rows = 0
    expected_schema = manifest["source_schema"]
    source_format = manifest["source_format"]
    for expected_sequence, entry in enumerate(manifest["source_entries"], start=1):
        if entry.get("entry_sequence") != expected_sequence:
            raise EvidenceCompactionError("Compaction source entries are not contiguous.")
        relative = _safe_relative(
            entry["source_relative_path"], "source_relative_path"
        )
        source = _resolved_under(root, relative, name="compaction source", must_exist=True)
        if source.is_symlink() or not source.is_file():
            raise EvidenceCompactionError(
                f"Compaction source is not a regular file: {relative}."
            )
        size = _non_negative_int(entry["source_size_bytes"], "source_size_bytes")
        if source.stat().st_size != size or _sha256_file(source) != entry["source_sha256"]:
            raise EvidenceCompactionError(
                f"Compaction source changed after staging: {relative}."
            )
        frame = _read_frame(source, source_format)
        if _schema(frame) != expected_schema or len(frame) != int(
            entry["source_row_count"]
        ):
            raise EvidenceCompactionError(
                f"Compaction source schema or row count changed: {relative}."
            )
        frames.append(frame)
        total_bytes += size
        total_rows += len(frame)
    combined = pd.concat(frames, ignore_index=True)
    output_relative = _safe_relative(
        manifest["compacted_output_relative_path"],
        "compacted_output_relative_path",
    )
    output = _resolved_under(
        root, output_relative, name="compacted output", must_exist=True
    )
    if output.is_symlink() or not output.is_file():
        raise EvidenceCompactionError("Compacted output must be a regular file.")
    if output.stat().st_size != manifest["compacted_output_size_bytes"] or _sha256_file(
        output
    ) != manifest["compacted_output_sha256"]:
        raise EvidenceCompactionError("Compacted output file identity is invalid.")
    verified = pd.read_parquet(output)
    if (
        _schema(verified) != expected_schema
        or len(verified) != total_rows
        or _frame_fingerprint(verified) != manifest["compacted_output_data_sha256"]
        or _frame_fingerprint(combined) != manifest["compacted_output_data_sha256"]
    ):
        raise EvidenceCompactionError("Compacted output is not equivalent to its sources.")
    if total_bytes != manifest["source_total_bytes"] or total_rows != manifest[
        "source_total_rows"
    ]:
        raise EvidenceCompactionError("Compaction manifest source totals are inconsistent.")
    return {
        "operation_id": manifest["operation_id"],
        "plan_id": manifest["plan_id"],
        "group_id": manifest["group_id"],
        "verification_status": "verified",
        "verified_source_file_count": len(frames),
        "verified_source_row_count": total_rows,
        "verified_output_row_count": len(verified),
        "source_files_mutated": False,
        "source_files_permanently_deleted": False,
        "replacement_authorized": False,
    }
