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
QUARANTINE_MANIFEST_VERSION = "evidence-quarantine-manifest-v1"
RESTORE_EVENT_VERSION = "evidence-restore-event-v1"
PLAN_ID_PATTERN = re.compile(r"^elp-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

PLAN_REQUIRED_COLUMNS = {
    "plan_id",
    "plan_created_at_utc",
    "relative_path",
    "category",
    "sha256",
    "size_bytes",
    "modified_at_utc",
    "protected_by_candidate",
    "always_protect",
    "planned_action",
    "requires_explicit_apply",
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


class EvidenceQuarantineError(ValueError):
    """Raised when a quarantine or restore operation is unsafe."""


def _text(value: Any, name: str) -> str:
    if value is None:
        raise EvidenceQuarantineError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise EvidenceQuarantineError(f"{name} must be non-empty.")
    return text


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    raise EvidenceQuarantineError(f"{name} must be boolean.")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise EvidenceQuarantineError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceQuarantineError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise EvidenceQuarantineError(f"{name} must be a non-negative integer.")
    return parsed


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceQuarantineError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise EvidenceQuarantineError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


def _safe_relative(value: Any, name: str) -> Path:
    text = _text(value, name).replace("\\", "/")
    path = Path(text)
    if path.is_absolute() or ".." in path.parts or text.startswith("/"):
        raise EvidenceQuarantineError(f"{name} must be a safe relative path.")
    if any(part in {"", "."} for part in path.parts):
        raise EvidenceQuarantineError(f"{name} must be a normalized relative path.")
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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


def _root(path: Path, name: str) -> Path:
    path = Path(path)
    if path.is_symlink():
        raise EvidenceQuarantineError(f"{name} cannot be a symbolic link.")
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise EvidenceQuarantineError(f"{name} must be an existing directory.")
    return resolved


def _reject_symlink_components(root: Path, relative: Path, name: str) -> None:
    current = root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise EvidenceQuarantineError(
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
        raise EvidenceQuarantineError(f"{name} does not exist: {relative}.") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvidenceQuarantineError(f"{name} escapes the data root: {relative}.") from exc
    return resolved


def _plan_id(plan: pd.DataFrame, summary: dict[str, Any]) -> str:
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


def prepare_quarantine_candidates(
    plan: pd.DataFrame,
    summary: dict[str, Any],
    *,
    confirm_plan_id: str,
) -> tuple[pd.DataFrame, str]:
    """Validate a lifecycle plan and return its explicit quarantine candidates."""
    missing = sorted(PLAN_REQUIRED_COLUMNS - set(plan.columns))
    if missing:
        raise EvidenceQuarantineError(
            "Lifecycle plan is missing required columns: " + ", ".join(missing) + "."
        )
    missing_summary = sorted(SUMMARY_REQUIRED_FIELDS - set(summary))
    if missing_summary:
        raise EvidenceQuarantineError(
            "Lifecycle summary is missing required fields: "
            + ", ".join(missing_summary)
            + "."
        )
    if summary["summary_contract_version"] != SUMMARY_CONTRACT_VERSION:
        raise EvidenceQuarantineError("Unsupported lifecycle summary contract.")
    if _boolean(summary["mutation_performed"], "mutation_performed"):
        raise EvidenceQuarantineError(
            "Quarantine can be applied only from a non-mutating lifecycle plan."
        )
    plan_ids = sorted({_text(value, "plan_id") for value in plan["plan_id"]})
    if len(plan_ids) != 1:
        raise EvidenceQuarantineError("Lifecycle plan must contain exactly one plan_id.")
    plan_id = plan_ids[0]
    if not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise EvidenceQuarantineError("Lifecycle plan_id is malformed.")
    if _text(summary["plan_id"], "summary.plan_id") != plan_id:
        raise EvidenceQuarantineError("Lifecycle plan and summary IDs do not match.")
    if _text(confirm_plan_id, "confirm_plan_id") != plan_id:
        raise EvidenceQuarantineError(
            "confirm_plan_id must exactly match the lifecycle plan."
        )
    if set(plan["plan_contract_version"].astype(str)) != {PLAN_CONTRACT_VERSION}:
        raise EvidenceQuarantineError("Unsupported lifecycle plan contract.")
    if not SHA256_PATTERN.fullmatch(_text(summary["policy_sha256"], "policy_sha256")):
        raise EvidenceQuarantineError("Lifecycle policy SHA-256 is malformed.")
    computed = _plan_id(plan, summary)
    if computed != plan_id:
        raise EvidenceQuarantineError(
            "Lifecycle plan identity does not match its content."
        )
    selected = plan.loc[plan["planned_action"] == "quarantine_candidate"].copy()
    if selected.empty:
        raise EvidenceQuarantineError(
            "Lifecycle plan contains no quarantine_candidate rows."
        )
    normalized_paths: list[str] = []
    for row in selected.itertuples(index=False):
        relative = _safe_relative(row.relative_path, "relative_path").as_posix()
        digest = _text(row.sha256, "sha256").lower()
        if not SHA256_PATTERN.fullmatch(digest):
            raise EvidenceQuarantineError(
                f"Lifecycle plan contains a malformed SHA-256 for {relative}."
            )
        _non_negative_int(row.size_bytes, "size_bytes")
        if _boolean(row.protected_by_candidate, "protected_by_candidate"):
            raise EvidenceQuarantineError(
                f"Candidate-protected evidence cannot be quarantined: {relative}."
            )
        if _boolean(row.always_protect, "always_protect"):
            raise EvidenceQuarantineError(
                f"Always-protected evidence cannot be quarantined: {relative}."
            )
        if not _boolean(row.requires_explicit_apply, "requires_explicit_apply"):
            raise EvidenceQuarantineError(
                f"Quarantine candidate lacks explicit-apply evidence: {relative}."
            )
        normalized_paths.append(relative)
    if len(normalized_paths) != len(set(normalized_paths)):
        raise EvidenceQuarantineError("Lifecycle plan contains duplicate source paths.")
    selected["relative_path"] = normalized_paths
    selected["sha256"] = selected["sha256"].astype(str).str.lower()
    return selected.sort_values("relative_path").reset_index(drop=True), plan_id


def _preflight(
    data_root: Path,
    candidates: pd.DataFrame,
    plan_id: str,
    *,
    quarantine_prefix: str = "quarantine",
) -> list[dict[str, Any]]:
    root = _root(data_root, "data_root")
    prefix = _safe_relative(quarantine_prefix, "quarantine_prefix")
    quarantine_root = _resolved_under(
        root, prefix / plan_id, name="quarantine root", must_exist=False
    )
    entries: list[dict[str, Any]] = []
    destination_paths: set[Path] = set()
    for sequence, row in enumerate(candidates.itertuples(index=False), start=1):
        relative = _safe_relative(row.relative_path, "relative_path")
        source = _resolved_under(root, relative, name="source evidence", must_exist=True)
        if source.is_symlink() or not source.is_file():
            raise EvidenceQuarantineError(
                f"Source evidence must be a regular non-symlink file: {relative}."
            )
        size = _non_negative_int(row.size_bytes, "size_bytes")
        if source.stat().st_size != size:
            raise EvidenceQuarantineError(
                f"Source size changed after lifecycle planning: {relative}."
            )
        digest = _sha256_file(source)
        if digest != row.sha256:
            raise EvidenceQuarantineError(
                f"Source content changed after lifecycle planning: {relative}."
            )
        destination_relative = prefix / plan_id / "files" / relative
        destination = _resolved_under(
            root,
            destination_relative,
            name="quarantine destination",
            must_exist=False,
        )
        if destination in destination_paths:
            raise EvidenceQuarantineError("Quarantine destinations are not unique.")
        destination_paths.add(destination)
        if destination.exists():
            raise EvidenceQuarantineError(
                f"Quarantine destination already exists: {destination_relative}."
            )
        entries.append(
            {
                "entry_sequence": sequence,
                "category": _text(row.category, "category"),
                "source_relative_path": relative.as_posix(),
                "quarantine_relative_path": destination_relative.as_posix(),
                "size_bytes": size,
                "sha256": digest,
                "original_modified_at_utc": _utc_iso(
                    row.modified_at_utc, "modified_at_utc"
                ),
                "_source": source,
                "_destination": destination,
                "_quarantine_root": quarantine_root,
            }
        )
    return entries


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


def quarantine_evidence(
    data_root: Path,
    candidates: pd.DataFrame,
    *,
    plan_id: str,
    actor: str,
    reason: str,
    applied_at_utc: Any | None = None,
    quarantine_prefix: str = "quarantine",
) -> tuple[dict[str, Any], Path]:
    """Atomically move verified candidates into reversible quarantine."""
    root = _root(data_root, "data_root")
    actor = _text(actor, "actor")
    reason = _text(reason, "reason")
    applied_at = _utc_iso(
        applied_at_utc or datetime.now(timezone.utc), "applied_at_utc"
    )
    entries = _preflight(
        root, candidates, plan_id, quarantine_prefix=quarantine_prefix
    )
    public_entries = [
        {key: value for key, value in entry.items() if not key.startswith("_")}
        for entry in entries
    ]
    operation_material = {
        "plan_id": plan_id,
        "applied_at_utc": applied_at,
        "actor": actor,
        "reason": reason,
        "entries": public_entries,
    }
    operation_id = "qtn-" + _digest(operation_material)[:24]
    manifest = {
        "operation_id": operation_id,
        "plan_id": plan_id,
        "applied_at_utc": applied_at,
        "actor": actor,
        "reason": reason,
        "data_root_name": root.name,
        "quarantine_root_relative_path": (
            _safe_relative(quarantine_prefix, "quarantine_prefix") / plan_id
        ).as_posix(),
        "file_count": len(public_entries),
        "total_bytes": sum(entry["size_bytes"] for entry in public_entries),
        "entries": public_entries,
        "permanent_deletion_performed": False,
        "restore_available": True,
        "manifest_contract_version": QUARANTINE_MANIFEST_VERSION,
    }
    manifest["manifest_hash"] = _digest(
        manifest, exclude=("manifest_hash",)
    )
    manifest_path = (
        entries[0]["_quarantine_root"]
        / f"quarantine_manifest_{operation_id}.json"
    )
    if manifest_path.exists() or manifest_path.with_suffix(".tmp").exists():
        raise FileExistsError(f"Quarantine manifest already exists: {manifest_path}.")
    moved: list[dict[str, Any]] = []
    try:
        for entry in entries:
            source = entry["_source"]
            destination = entry["_destination"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.replace(destination)
            moved.append(entry)
            if source.exists() or not destination.is_file():
                raise EvidenceQuarantineError(
                    f"Quarantine move did not complete for {entry['source_relative_path']}."
                )
            if (
                destination.stat().st_size != entry["size_bytes"]
                or _sha256_file(destination) != entry["sha256"]
            ):
                raise EvidenceQuarantineError(
                    f"Quarantine verification failed for {entry['source_relative_path']}."
                )
        _write_json_exclusive(manifest, manifest_path)
    except Exception:
        for entry in reversed(moved):
            source = entry["_source"]
            destination = entry["_destination"]
            if destination.exists() and not source.exists():
                source.parent.mkdir(parents=True, exist_ok=True)
                destination.replace(source)
        raise
    return manifest, manifest_path


def load_quarantine_manifest(path: Path) -> dict[str, Any]:
    path = Path(path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("manifest_contract_version") != QUARANTINE_MANIFEST_VERSION:
        raise EvidenceQuarantineError("Unsupported quarantine manifest contract.")
    if manifest.get("permanent_deletion_performed") is not False:
        raise EvidenceQuarantineError("Quarantine manifest claims permanent deletion.")
    if manifest.get("restore_available") is not True:
        raise EvidenceQuarantineError("Quarantine manifest must remain restorable.")
    if manifest.get("manifest_hash") != _digest(
        manifest, exclude=("manifest_hash",)
    ):
        raise EvidenceQuarantineError("Quarantine manifest hash is invalid.")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise EvidenceQuarantineError("Quarantine manifest has no entries.")
    if manifest.get("file_count") != len(entries):
        raise EvidenceQuarantineError("Quarantine manifest file count is inconsistent.")
    paths: set[str] = set()
    total = 0
    for expected_sequence, entry in enumerate(entries, start=1):
        if entry.get("entry_sequence") != expected_sequence:
            raise EvidenceQuarantineError("Quarantine entries are not contiguous.")
        source = _safe_relative(
            entry.get("source_relative_path"), "source_relative_path"
        ).as_posix()
        destination = _safe_relative(
            entry.get("quarantine_relative_path"), "quarantine_relative_path"
        ).as_posix()
        if source in paths or destination in paths:
            raise EvidenceQuarantineError("Quarantine manifest paths are duplicated.")
        paths.update({source, destination})
        digest = _text(entry.get("sha256"), "sha256").lower()
        if not SHA256_PATTERN.fullmatch(digest):
            raise EvidenceQuarantineError("Quarantine manifest contains a bad SHA-256.")
        total += _non_negative_int(entry.get("size_bytes"), "size_bytes")
        _utc_iso(entry.get("original_modified_at_utc"), "original_modified_at_utc")
    if manifest.get("total_bytes") != total:
        raise EvidenceQuarantineError("Quarantine manifest byte total is inconsistent.")
    return manifest


def verify_quarantine_state(
    data_root: Path, manifest: dict[str, Any]
) -> dict[str, Any]:
    """Verify that every entry is wholly quarantined or wholly restored."""
    root = _root(data_root, "data_root")
    states: list[str] = []
    for entry in manifest["entries"]:
        source_relative = _safe_relative(
            entry["source_relative_path"], "source_relative_path"
        )
        destination_relative = _safe_relative(
            entry["quarantine_relative_path"], "quarantine_relative_path"
        )
        source = _resolved_under(
            root, source_relative, name="source evidence", must_exist=False
        )
        destination = _resolved_under(
            root,
            destination_relative,
            name="quarantine evidence",
            must_exist=False,
        )
        source_exists = source.exists()
        destination_exists = destination.exists()
        if source_exists == destination_exists:
            raise EvidenceQuarantineError(
                f"Evidence state is ambiguous for {source_relative}."
            )
        existing = source if source_exists else destination
        if existing.is_symlink() or not existing.is_file():
            raise EvidenceQuarantineError(
                f"Evidence state contains a non-regular file: {existing}."
            )
        if (
            existing.stat().st_size != entry["size_bytes"]
            or _sha256_file(existing) != entry["sha256"]
        ):
            raise EvidenceQuarantineError(
                f"Evidence content does not match manifest: {source_relative}."
            )
        states.append("restored" if source_exists else "quarantined")
    if len(set(states)) != 1:
        raise EvidenceQuarantineError("Quarantine operation is in a mixed state.")
    return {
        "operation_id": manifest["operation_id"],
        "plan_id": manifest["plan_id"],
        "state": states[0],
        "verified_file_count": len(states),
        "verified_total_bytes": int(manifest["total_bytes"]),
        "permanent_deletion_performed": False,
    }


def restore_evidence(
    data_root: Path,
    manifest: dict[str, Any],
    *,
    confirm_operation_id: str,
    actor: str,
    reason: str,
    restored_at_utc: Any | None = None,
) -> tuple[dict[str, Any], Path]:
    """Restore a complete verified quarantine operation without overwriting sources."""
    root = _root(data_root, "data_root")
    operation_id = _text(manifest.get("operation_id"), "operation_id")
    if _text(confirm_operation_id, "confirm_operation_id") != operation_id:
        raise EvidenceQuarantineError(
            "confirm_operation_id must exactly match the quarantine manifest."
        )
    state = verify_quarantine_state(root, manifest)
    if state["state"] != "quarantined":
        raise EvidenceQuarantineError("Only fully quarantined evidence can be restored.")
    actor = _text(actor, "actor")
    reason = _text(reason, "reason")
    restored_at = _utc_iso(
        restored_at_utc or datetime.now(timezone.utc), "restored_at_utc"
    )
    material = {
        "operation_id": operation_id,
        "restored_at_utc": restored_at,
        "actor": actor,
        "reason": reason,
        "quarantine_manifest_hash": manifest["manifest_hash"],
    }
    restore_id = "rst-" + _digest(material)[:24]
    event = {
        "restore_id": restore_id,
        "operation_id": operation_id,
        "plan_id": manifest["plan_id"],
        "restored_at_utc": restored_at,
        "actor": actor,
        "reason": reason,
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
        "quarantine_manifest_hash": manifest["manifest_hash"],
        "permanent_deletion_performed": False,
        "restore_event_contract_version": RESTORE_EVENT_VERSION,
    }
    event["restore_event_hash"] = _digest(
        event, exclude=("restore_event_hash",)
    )
    quarantine_root = _resolved_under(
        root,
        _safe_relative(
            manifest["quarantine_root_relative_path"],
            "quarantine_root_relative_path",
        ),
        name="quarantine root",
        must_exist=True,
    )
    event_path = quarantine_root / f"restore_event_{operation_id}_{restore_id}.json"
    if event_path.exists() or event_path.with_suffix(".tmp").exists():
        raise FileExistsError(f"Restore event already exists: {event_path}.")
    preflight: list[tuple[Path, Path, dict[str, Any]]] = []
    for entry in manifest["entries"]:
        source = _resolved_under(
            root,
            _safe_relative(entry["source_relative_path"], "source_relative_path"),
            name="restore destination",
            must_exist=False,
        )
        quarantine = _resolved_under(
            root,
            _safe_relative(
                entry["quarantine_relative_path"], "quarantine_relative_path"
            ),
            name="quarantined evidence",
            must_exist=True,
        )
        if source.exists():
            raise EvidenceQuarantineError(
                f"Restore destination already exists: {entry['source_relative_path']}."
            )
        if quarantine.stat().st_size != entry["size_bytes"] or _sha256_file(
            quarantine
        ) != entry["sha256"]:
            raise EvidenceQuarantineError(
                f"Quarantined evidence changed: {entry['quarantine_relative_path']}."
            )
        preflight.append((quarantine, source, entry))
    moved: list[tuple[Path, Path, dict[str, Any]]] = []
    try:
        for quarantine, source, entry in preflight:
            source.parent.mkdir(parents=True, exist_ok=True)
            quarantine.replace(source)
            moved.append((quarantine, source, entry))
            if quarantine.exists() or not source.is_file():
                raise EvidenceQuarantineError(
                    f"Restore move did not complete for {entry['source_relative_path']}."
                )
            if source.stat().st_size != entry["size_bytes"] or _sha256_file(
                source
            ) != entry["sha256"]:
                raise EvidenceQuarantineError(
                    f"Restore verification failed for {entry['source_relative_path']}."
                )
        _write_json_exclusive(event, event_path)
    except Exception:
        for quarantine, source, _ in reversed(moved):
            if source.exists() and not quarantine.exists():
                quarantine.parent.mkdir(parents=True, exist_ok=True)
                source.replace(quarantine)
        raise
    verified = verify_quarantine_state(root, manifest)
    if verified["state"] != "restored":
        raise EvidenceQuarantineError("Restore did not reach a complete restored state.")
    return event, event_path
