from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import pandas as pd

from forecasting.model_registry import verify_candidate_history


BUNDLE_CONTRACT_VERSION = "candidate-evidence-bundle-v1"
RECOVERY_CONTRACT_VERSION = "candidate-evidence-recovery-v1"
REQUIRED_ROLES = {
    "promotion_summary",
    "comparison_predictions",
    "reconciliation_metrics",
    "provider_health_summary",
}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
BUNDLE_ID_PATTERN = re.compile(r"^ceb-[0-9a-f]{24}$")


class EvidenceBundleError(ValueError):
    """Raised when candidate evidence cannot be bundled or recovered safely."""


def _text(value: Any, name: str) -> str:
    if value is None:
        raise EvidenceBundleError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise EvidenceBundleError(f"{name} must be non-empty.")
    return text


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceBundleError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise EvidenceBundleError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


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


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(value: Any, name: str) -> Path:
    text = _text(value, name).replace("\\", "/")
    path = Path(text)
    if path.is_absolute() or text.startswith("/") or ".." in path.parts:
        raise EvidenceBundleError(f"{name} must be a safe relative path.")
    if any(part in {"", "."} for part in path.parts):
        raise EvidenceBundleError(f"{name} must be a normalized relative path.")
    return path


def _safe_archive_path(value: Any, name: str = "archive_path") -> str:
    text = _text(value, name)
    if "\\" in text:
        raise EvidenceBundleError(f"{name} must use forward slashes.")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise EvidenceBundleError(f"{name} must be a safe relative archive path.")
    if any(part in {"", "."} for part in path.parts):
        raise EvidenceBundleError(f"{name} must be normalized.")
    return path.as_posix()


def _root(path: Path, name: str) -> Path:
    path = Path(path)
    if path.is_symlink():
        raise EvidenceBundleError(f"{name} cannot be a symbolic link.")
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise EvidenceBundleError(f"{name} must be an existing directory.")
    return resolved


def _reject_symlink_components(root: Path, relative: Path, name: str) -> None:
    current = root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise EvidenceBundleError(
                f"{name} contains a symbolic-link component: {current}."
            )


def _source_under(root: Path, path: Path, name: str) -> tuple[Path, Path]:
    path = Path(path)
    if path.is_absolute():
        try:
            relative = path.resolve().relative_to(root)
        except ValueError as exc:
            raise EvidenceBundleError(f"{name} is outside data_root: {path}.") from exc
    else:
        relative = _safe_relative(path, name)
    _reject_symlink_components(root, relative, name)
    try:
        resolved = (root / relative).resolve(strict=True)
    except FileNotFoundError as exc:
        raise EvidenceBundleError(f"{name} does not exist: {relative}.") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvidenceBundleError(f"{name} escapes data_root: {relative}.") from exc
    if resolved.is_symlink() or not resolved.is_file():
        raise EvidenceBundleError(f"{name} must be a regular non-symlink file.")
    return relative, resolved


def _candidate_directory(root: Path, path: Path) -> tuple[Path, Path]:
    path = Path(path)
    if path.is_absolute():
        try:
            relative = path.resolve().relative_to(root)
        except ValueError as exc:
            raise EvidenceBundleError("candidate_dir must be inside data_root.") from exc
    else:
        relative = _safe_relative(path, "candidate_dir")
    _reject_symlink_components(root, relative, "candidate_dir")
    try:
        resolved = (root / relative).resolve(strict=True)
    except FileNotFoundError as exc:
        raise EvidenceBundleError("candidate_dir does not exist.") from exc
    if not resolved.is_dir() or resolved.is_symlink():
        raise EvidenceBundleError("candidate_dir must be a non-symlink directory.")
    return relative, resolved


def _frame_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    suffix = Path(name).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(io.BytesIO(data))
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(io.BytesIO(data))
    if suffix == ".json":
        payload = json.loads(data.decode("utf-8"))
        return pd.DataFrame([payload] if isinstance(payload, dict) else payload)
    raise EvidenceBundleError(
        f"Evidence role file must be CSV, Parquet, or JSON: {name}."
    )


def _unique_text(frame: pd.DataFrame, column: str, role: str) -> set[str]:
    if column not in frame.columns:
        raise EvidenceBundleError(f"{role} is missing {column}.")
    values = {str(value).strip() for value in frame[column].dropna()}
    if not values or "" in values:
        raise EvidenceBundleError(f"{role}.{column} must be non-empty.")
    return values


def _validate_role(role: str, name: str, data: bytes, candidate: dict[str, Any]) -> None:
    frame = _frame_from_bytes(name, data)
    if frame.empty:
        raise EvidenceBundleError(f"{role} evidence must not be empty.")
    if role == "promotion_summary":
        expected = {
            "assessment_id": candidate["promotion_assessment_id"],
            "comparison_run_id": candidate["comparison_run_id"],
            "reconciliation_run_id": candidate["reconciliation_run_id"],
        }
    elif role == "comparison_predictions":
        expected = {"run_id": candidate["comparison_run_id"]}
    elif role == "reconciliation_metrics":
        expected = {"reconciliation_run_id": candidate["reconciliation_run_id"]}
    elif role == "provider_health_summary":
        expected = {"monitor_run_id": candidate["provider_health_monitor_run_id"]}
    else:
        raise EvidenceBundleError(f"Unsupported required evidence role: {role}.")
    for column, value in expected.items():
        values = _unique_text(frame, column, role)
        if values != {str(value)}:
            raise EvidenceBundleError(
                f"{role}.{column} does not match the approved candidate."
            )


def _candidate_history_from_payloads(
    payloads: dict[str, bytes], candidate_id: str
) -> dict[str, Any]:
    prefix = f"candidate/{candidate_id}/"
    manifests: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for name, data in sorted(payloads.items()):
        if not name.startswith(prefix):
            continue
        file_name = PurePosixPath(name).name
        payload = json.loads(data.decode("utf-8"))
        if file_name.startswith("manifest_v"):
            manifests.append(payload)
        elif file_name.startswith("event_v"):
            events.append(payload)
        else:
            raise EvidenceBundleError(
                f"Candidate history contains an unexpected file: {file_name}."
            )
    try:
        latest = verify_candidate_history(manifests, events)
    except Exception as exc:
        raise EvidenceBundleError(
            f"Bundled candidate history failed verification: {exc}"
        ) from exc
    if latest["candidate_id"] != candidate_id:
        raise EvidenceBundleError("Bundled candidate history has the wrong ID.")
    return latest


def _tar_bytes(payloads: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name in sorted(payloads):
            safe_name = _safe_archive_path(name)
            data = payloads[name]
            info = tarfile.TarInfo(safe_name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def create_evidence_bundle(
    data_root: Path,
    candidate_dir: Path,
    role_paths: dict[str, Path],
    *,
    extra_paths: Iterable[Path] = (),
    actor: str,
    reason: str,
    created_at_utc: Any | None = None,
    output_root: Path | None = None,
    max_bundle_bytes: int = 536_870_912,
) -> tuple[dict[str, Any], Path]:
    """Create a deterministic approved-candidate evidence tar archive."""
    root = _root(data_root, "data_root")
    actor = _text(actor, "actor")
    reason = _text(reason, "reason")
    created_at = _utc_iso(
        created_at_utc or datetime.now(timezone.utc), "created_at_utc"
    )
    if isinstance(max_bundle_bytes, bool) or int(max_bundle_bytes) < 1:
        raise EvidenceBundleError("max_bundle_bytes must be a positive integer.")
    max_bundle_bytes = int(max_bundle_bytes)
    candidate_relative, candidate_path = _candidate_directory(root, candidate_dir)
    manifest_files = sorted(candidate_path.glob("manifest_v*.json"))
    event_files = sorted(candidate_path.glob("event_v*.json"))
    manifests = [json.loads(path.read_text(encoding="utf-8")) for path in manifest_files]
    events = [json.loads(path.read_text(encoding="utf-8")) for path in event_files]
    try:
        candidate = verify_candidate_history(manifests, events)
    except Exception as exc:
        raise EvidenceBundleError(f"Candidate history failed verification: {exc}") from exc
    if candidate["candidate_state"] != "approved":
        raise EvidenceBundleError(
            "Only an approved candidate can be packaged for recovery."
        )
    if candidate.get("deployment_authorized") is not False or candidate.get(
        "active_model_unchanged"
    ) is not True:
        raise EvidenceBundleError("Candidate safety flags are invalid.")
    if set(role_paths) != REQUIRED_ROLES:
        missing = sorted(REQUIRED_ROLES - set(role_paths))
        extra = sorted(set(role_paths) - REQUIRED_ROLES)
        raise EvidenceBundleError(
            f"Required evidence roles are incomplete; missing={missing}, extra={extra}."
        )
    payloads: dict[str, bytes] = {}
    entries: list[dict[str, Any]] = []
    source_paths: set[Path] = set()

    def add_file(role: str, source_relative: Path, source: Path, archive_path: str) -> None:
        if source in source_paths:
            raise EvidenceBundleError(f"Evidence source is supplied more than once: {source}.")
        source_paths.add(source)
        archive_name = _safe_archive_path(archive_path)
        if archive_name in payloads or archive_name == "bundle_manifest.json":
            raise EvidenceBundleError(f"Duplicate bundle archive path: {archive_name}.")
        data = source.read_bytes()
        payloads[archive_name] = data
        entries.append(
            {
                "entry_sequence": len(entries) + 1,
                "role": role,
                "archive_path": archive_name,
                "source_relative_path": source_relative.as_posix(),
                "size_bytes": len(data),
                "sha256": _sha256_bytes(data),
            }
        )

    for path in [*manifest_files, *event_files]:
        relative = path.relative_to(root)
        archive_path = f"candidate/{candidate['candidate_id']}/{path.name}"
        add_file("candidate_history", relative, path, archive_path)
    for role in sorted(REQUIRED_ROLES):
        relative, source = _source_under(root, role_paths[role], role)
        data = source.read_bytes()
        _validate_role(role, source.name, data, candidate)
        add_file(role, relative, source, f"evidence/{role}/{relative.as_posix()}")
    for path in extra_paths:
        relative, source = _source_under(root, Path(path), "extra_evidence")
        add_file("extra_evidence", relative, source, f"evidence/extra/{relative.as_posix()}")
    total_source_bytes = sum(entry["size_bytes"] for entry in entries)
    if total_source_bytes > max_bundle_bytes:
        raise EvidenceBundleError(
            f"Bundle sources total {total_source_bytes} bytes, above max_bundle_bytes={max_bundle_bytes}."
        )
    core = {
        "created_at_utc": created_at,
        "actor": actor,
        "reason": reason,
        "candidate_id": candidate["candidate_id"],
        "candidate_version": candidate["candidate_version"],
        "repository": candidate["repository"],
        "code_commit_sha": candidate["code_commit_sha"],
        "code_tree_sha": candidate["code_tree_sha"],
        "promotion_assessment_id": candidate["promotion_assessment_id"],
        "comparison_run_id": candidate["comparison_run_id"],
        "reconciliation_run_id": candidate["reconciliation_run_id"],
        "provider_health_monitor_run_id": candidate["provider_health_monitor_run_id"],
        "entries": entries,
    }
    bundle_id = "ceb-" + _digest(core)[:24]
    manifest = {
        "bundle_id": bundle_id,
        "created_at_utc": created_at,
        "actor": actor,
        "reason": reason,
        "candidate_id": candidate["candidate_id"],
        "candidate_state": candidate["candidate_state"],
        "candidate_version": candidate["candidate_version"],
        "repository": candidate["repository"],
        "code_commit_sha": candidate["code_commit_sha"],
        "code_tree_sha": candidate["code_tree_sha"],
        "promotion_assessment_id": candidate["promotion_assessment_id"],
        "comparison_run_id": candidate["comparison_run_id"],
        "reconciliation_run_id": candidate["reconciliation_run_id"],
        "provider_health_monitor_run_id": candidate[
            "provider_health_monitor_run_id"
        ],
        "entry_count": len(entries),
        "total_entry_bytes": total_source_bytes,
        "entries": entries,
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "source_files_mutated": False,
        "bundle_contract_version": BUNDLE_CONTRACT_VERSION,
    }
    manifest["manifest_hash"] = _digest(manifest, exclude=("manifest_hash",))
    payloads["bundle_manifest.json"] = (
        json.dumps(_canonical(manifest), indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    tar_data = _tar_bytes(payloads)
    if len(tar_data) > max_bundle_bytes + 10_485_760:
        raise EvidenceBundleError("Tar overhead exceeded the permitted bundle bound.")
    output_base = Path(output_root) if output_root is not None else root / "bundles"
    if not output_base.is_absolute():
        output_base = root / output_base
    output_base.parent.mkdir(parents=True, exist_ok=True)
    if output_base.exists() and output_base.is_symlink():
        raise EvidenceBundleError("output_root cannot be a symbolic link.")
    output_directory = output_base / candidate["candidate_id"]
    output_path = output_directory / f"evidence_bundle_{bundle_id}.tar"
    temporary = output_path.with_suffix(".tmp")
    for path in (output_path, temporary):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    output_directory.mkdir(parents=True, exist_ok=True)
    try:
        with temporary.open("xb") as handle:
            handle.write(tar_data)
        temporary.replace(output_path)
        verified, _ = verify_evidence_bundle(output_path, max_bundle_bytes=max_bundle_bytes)
        if verified["bundle_id"] != bundle_id:
            raise EvidenceBundleError("Created bundle failed identity verification.")
    except Exception:
        temporary.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        raise
    return manifest, output_path


def _read_bundle_payloads(
    bundle_path: Path, *, max_bundle_bytes: int
) -> tuple[dict[str, bytes], str]:
    path = Path(bundle_path)
    if path.is_symlink() or not path.is_file():
        raise EvidenceBundleError("bundle_path must be a regular non-symlink file.")
    if path.stat().st_size > max_bundle_bytes + 10_485_760:
        raise EvidenceBundleError("Bundle exceeds the configured size bound.")
    payloads: dict[str, bytes] = {}
    with tarfile.open(path, mode="r:") as archive:
        for member in archive.getmembers():
            name = _safe_archive_path(member.name, "tar member name")
            if not member.isfile() or member.issym() or member.islnk():
                raise EvidenceBundleError(
                    f"Bundle contains a non-regular member: {name}."
                )
            if name in payloads:
                raise EvidenceBundleError(f"Bundle contains duplicate member: {name}.")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise EvidenceBundleError(f"Bundle member cannot be read: {name}.")
            data = extracted.read(max_bundle_bytes + 1)
            if len(data) != member.size or len(data) > max_bundle_bytes:
                raise EvidenceBundleError(f"Bundle member size is invalid: {name}.")
            payloads[name] = data
    return payloads, _sha256_file(path)


def verify_evidence_bundle(
    bundle_path: Path, *, max_bundle_bytes: int = 536_870_912
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify archive safety, hashes, evidence identities, and candidate history."""
    payloads, bundle_sha = _read_bundle_payloads(
        bundle_path, max_bundle_bytes=max_bundle_bytes
    )
    if "bundle_manifest.json" not in payloads:
        raise EvidenceBundleError("Bundle is missing bundle_manifest.json.")
    manifest = json.loads(payloads["bundle_manifest.json"].decode("utf-8"))
    if manifest.get("bundle_contract_version") != BUNDLE_CONTRACT_VERSION:
        raise EvidenceBundleError("Unsupported bundle contract.")
    if manifest.get("deployment_authorized") is not False:
        raise EvidenceBundleError("Bundle cannot authorize deployment.")
    if manifest.get("active_model_unchanged") is not True:
        raise EvidenceBundleError("Bundle must retain the active model unchanged.")
    if manifest.get("source_files_mutated") is not False:
        raise EvidenceBundleError("Bundle cannot claim source mutation.")
    if manifest.get("manifest_hash") != _digest(
        manifest, exclude=("manifest_hash",)
    ):
        raise EvidenceBundleError("Bundle manifest hash is invalid.")
    bundle_id = _text(manifest.get("bundle_id"), "bundle_id")
    if not BUNDLE_ID_PATTERN.fullmatch(bundle_id):
        raise EvidenceBundleError("Bundle ID is malformed.")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise EvidenceBundleError("Bundle manifest has no entries.")
    if manifest.get("entry_count") != len(entries):
        raise EvidenceBundleError("Bundle entry count is inconsistent.")
    expected_names = {"bundle_manifest.json"}
    role_payloads: dict[str, tuple[str, bytes]] = {}
    total = 0
    for sequence, entry in enumerate(entries, start=1):
        if entry.get("entry_sequence") != sequence:
            raise EvidenceBundleError("Bundle entries are not contiguous.")
        archive_path = _safe_archive_path(entry.get("archive_path"))
        if archive_path in expected_names:
            raise EvidenceBundleError("Bundle entry archive paths are duplicated.")
        expected_names.add(archive_path)
        if archive_path not in payloads:
            raise EvidenceBundleError(f"Bundle entry is missing: {archive_path}.")
        data = payloads[archive_path]
        if len(data) != entry.get("size_bytes") or _sha256_bytes(data) != entry.get(
            "sha256"
        ):
            raise EvidenceBundleError(f"Bundle entry identity is invalid: {archive_path}.")
        total += len(data)
        role = _text(entry.get("role"), "role")
        if role in REQUIRED_ROLES:
            if role in role_payloads:
                raise EvidenceBundleError(f"Bundle contains duplicate role: {role}.")
            role_payloads[role] = (archive_path, data)
    if set(payloads) != expected_names:
        unexpected = sorted(set(payloads) - expected_names)
        raise EvidenceBundleError(f"Bundle contains unexpected members: {unexpected}.")
    if total != manifest.get("total_entry_bytes"):
        raise EvidenceBundleError("Bundle byte total is inconsistent.")
    if set(role_payloads) != REQUIRED_ROLES:
        raise EvidenceBundleError("Bundle does not contain all required evidence roles.")
    candidate_id = _text(manifest.get("candidate_id"), "candidate_id")
    candidate = _candidate_history_from_payloads(payloads, candidate_id)
    if candidate["candidate_state"] != "approved":
        raise EvidenceBundleError("Bundled candidate is not approved.")
    candidate_fields = (
        "candidate_version",
        "repository",
        "code_commit_sha",
        "code_tree_sha",
        "promotion_assessment_id",
        "comparison_run_id",
        "reconciliation_run_id",
        "provider_health_monitor_run_id",
    )
    for field in candidate_fields:
        if manifest.get(field) != candidate.get(field):
            raise EvidenceBundleError(
                f"Bundle manifest does not match candidate field {field}."
            )
    for role, (name, data) in role_payloads.items():
        _validate_role(role, name, data, candidate)
    return manifest, {
        "bundle_id": bundle_id,
        "bundle_sha256": bundle_sha,
        "verification_status": "verified",
        "verified_entry_count": len(entries),
        "verified_total_entry_bytes": total,
        "candidate_id": candidate_id,
        "candidate_state": candidate["candidate_state"],
        "deployment_authorized": False,
        "active_model_unchanged": True,
    }


def recover_evidence_bundle(
    bundle_path: Path,
    destination: Path,
    *,
    confirm_bundle_id: str,
    actor: str,
    reason: str,
    recovered_at_utc: Any | None = None,
    max_bundle_bytes: int = 536_870_912,
) -> tuple[dict[str, Any], Path]:
    """Recover a verified bundle into a new directory and verify every file."""
    manifest, verification = verify_evidence_bundle(
        bundle_path, max_bundle_bytes=max_bundle_bytes
    )
    if _text(confirm_bundle_id, "confirm_bundle_id") != manifest["bundle_id"]:
        raise EvidenceBundleError("confirm_bundle_id must exactly match the bundle.")
    actor = _text(actor, "actor")
    reason = _text(reason, "reason")
    recovered_at = _utc_iso(
        recovered_at_utc or datetime.now(timezone.utc), "recovered_at_utc"
    )
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise EvidenceBundleError("Recovery destination must not already exist.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink():
        raise EvidenceBundleError("Recovery destination parent cannot be a symlink.")
    temporary = destination.parent / f".{destination.name}.tmp-{manifest['bundle_id']}"
    if temporary.exists() or temporary.is_symlink():
        raise EvidenceBundleError("Recovery temporary directory already exists.")
    payloads, bundle_sha = _read_bundle_payloads(
        bundle_path, max_bundle_bytes=max_bundle_bytes
    )
    material = {
        "bundle_id": manifest["bundle_id"],
        "bundle_sha256": bundle_sha,
        "recovered_at_utc": recovered_at,
        "actor": actor,
        "reason": reason,
        "destination_name": destination.name,
    }
    recovery_id = "rcv-" + _digest(material)[:24]
    event = {
        "recovery_id": recovery_id,
        "bundle_id": manifest["bundle_id"],
        "recovered_at_utc": recovered_at,
        "actor": actor,
        "reason": reason,
        "destination_name": destination.name,
        "bundle_sha256": bundle_sha,
        "bundle_manifest_hash": manifest["manifest_hash"],
        "candidate_id": manifest["candidate_id"],
        "candidate_state": manifest["candidate_state"],
        "recovered_entry_count": manifest["entry_count"],
        "recovered_total_entry_bytes": manifest["total_entry_bytes"],
        "recovery_status": "verified",
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "source_files_mutated": False,
        "recovery_contract_version": RECOVERY_CONTRACT_VERSION,
    }
    event["recovery_event_hash"] = _digest(
        event, exclude=("recovery_event_hash",)
    )
    event_name = f"recovery_verification_{recovery_id}.json"
    try:
        temporary.mkdir(parents=True)
        for name, data in sorted(payloads.items()):
            safe = PurePosixPath(_safe_archive_path(name))
            output = temporary.joinpath(*safe.parts)
            output.parent.mkdir(parents=True, exist_ok=True)
            if output.exists():
                raise EvidenceBundleError(f"Recovery path collision: {name}.")
            with output.open("xb") as handle:
                handle.write(data)
        for entry in manifest["entries"]:
            output = temporary.joinpath(
                *PurePosixPath(entry["archive_path"]).parts
            )
            if output.stat().st_size != entry["size_bytes"] or _sha256_file(
                output
            ) != entry["sha256"]:
                raise EvidenceBundleError(
                    f"Recovered entry failed verification: {entry['archive_path']}."
                )
        event_path = temporary / event_name
        with event_path.open("x", encoding="utf-8") as handle:
            json.dump(_canonical(event), handle, indent=2, sort_keys=True)
            handle.write("\n")
        candidate_directory = temporary / "candidate" / manifest["candidate_id"]
        manifests = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(candidate_directory.glob("manifest_v*.json"))
        ]
        events = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(candidate_directory.glob("event_v*.json"))
        ]
        latest = verify_candidate_history(manifests, events)
        if latest["candidate_id"] != manifest["candidate_id"]:
            raise EvidenceBundleError("Recovered candidate history is inconsistent.")
        temporary.replace(destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    final_event_path = destination / event_name
    recovered_verification = verify_recovered_bundle(destination)
    if recovered_verification["recovery_id"] != recovery_id:
        raise EvidenceBundleError("Recovery verification identity is inconsistent.")
    return event, final_event_path


def verify_recovered_bundle(destination: Path) -> dict[str, Any]:
    """Verify a previously recovered bundle directory without its source archive."""
    root = Path(destination)
    if root.is_symlink() or not root.is_dir():
        raise EvidenceBundleError("Recovered bundle destination must be a directory.")
    manifest_path = root / "bundle_manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise EvidenceBundleError("Recovered bundle manifest is missing.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("bundle_contract_version") != BUNDLE_CONTRACT_VERSION or manifest.get(
        "manifest_hash"
    ) != _digest(manifest, exclude=("manifest_hash",)):
        raise EvidenceBundleError("Recovered bundle manifest is invalid.")
    expected = {"bundle_manifest.json"}
    payloads: dict[str, bytes] = {"bundle_manifest.json": manifest_path.read_bytes()}
    for entry in manifest["entries"]:
        archive_path = _safe_archive_path(entry["archive_path"])
        expected.add(archive_path)
        path = root.joinpath(*PurePosixPath(archive_path).parts)
        if path.is_symlink() or not path.is_file():
            raise EvidenceBundleError(f"Recovered entry is missing: {archive_path}.")
        data = path.read_bytes()
        if len(data) != entry["size_bytes"] or _sha256_bytes(data) != entry["sha256"]:
            raise EvidenceBundleError(f"Recovered entry changed: {archive_path}.")
        payloads[archive_path] = data
    event_paths = sorted(root.glob("recovery_verification_*.json"))
    if len(event_paths) != 1:
        raise EvidenceBundleError("Recovered bundle must contain one recovery event.")
    event = json.loads(event_paths[0].read_text(encoding="utf-8"))
    if event.get("recovery_contract_version") != RECOVERY_CONTRACT_VERSION or event.get(
        "recovery_event_hash"
    ) != _digest(event, exclude=("recovery_event_hash",)):
        raise EvidenceBundleError("Recovery event is invalid.")
    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    expected.add(event_paths[0].relative_to(root).as_posix())
    if actual_files != expected:
        raise EvidenceBundleError("Recovered bundle contains unexpected files.")
    candidate = _candidate_history_from_payloads(payloads, manifest["candidate_id"])
    if candidate["candidate_state"] != "approved":
        raise EvidenceBundleError("Recovered candidate is not approved.")
    if event.get("bundle_id") != manifest["bundle_id"] or event.get(
        "bundle_manifest_hash"
    ) != manifest["manifest_hash"]:
        raise EvidenceBundleError("Recovery event does not match the bundle manifest.")
    return {
        "recovery_id": event["recovery_id"],
        "bundle_id": manifest["bundle_id"],
        "candidate_id": manifest["candidate_id"],
        "candidate_state": candidate["candidate_state"],
        "verification_status": "verified",
        "verified_entry_count": manifest["entry_count"],
        "deployment_authorized": False,
        "active_model_unchanged": True,
        "source_files_mutated": False,
    }
