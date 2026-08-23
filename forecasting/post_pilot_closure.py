from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import tarfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from forecasting.fabric_pilot import FabricPilotError, verify_fabric_pilot_plan
from forecasting.fabric_pilot_authorization import (
    FabricPilotAuthorizationError,
    verify_fabric_pilot_authorization,
    verify_fabric_pilot_preflight,
)
from forecasting.fabric_pilot_receipt import (
    FabricPilotReceiptError,
    verify_fabric_pilot_run_assessment,
    verify_fabric_pilot_run_receipt,
)
from forecasting.post_pilot_decision import (
    PostPilotDecisionError,
    verify_post_pilot_decision,
)


MANIFEST_CONTRACT_VERSION = "post-pilot-closure-manifest-v1"
VERIFICATION_CONTRACT_VERSION = "post-pilot-closure-verification-v1"
CLOSURE_ID_PATTERN = re.compile(r"^pcl-[0-9a-f]{24}$")
VERIFICATION_ID_PATTERN = re.compile(r"^pcv-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
AUTOMATION_IDENTITY_PATTERN = re.compile(
    r"(^|[-_.])(bot|automation|github-actions|ci|runner|service-account)([-_.]|$)",
    re.IGNORECASE,
)
MANIFEST_ARCHIVE_PATH = "closure_manifest.json"
DEFAULT_MAX_MEMBERS = 1_000
DEFAULT_MAX_TOTAL_BYTES = 1_000_000_000
DEFAULT_MAX_MANIFEST_BYTES = 5_000_000
DOCUMENT_ROLES = (
    ("pilot_plan", "documents/pilot_plan.json"),
    ("pilot_preflight", "documents/pilot_preflight.json"),
    ("pilot_authorization", "documents/pilot_authorization.json"),
    ("pilot_run_receipt", "documents/pilot_run_receipt.json"),
    ("pilot_run_assessment", "documents/pilot_run_assessment.json"),
    ("post_pilot_decision", "documents/post_pilot_decision.json"),
)


class PostPilotClosureError(ValueError):
    """Raised when a closure archive or recovery is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise PostPilotClosureError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise PostPilotClosureError(f"{name} must be non-empty.")
    return text


def _human_identity(value: Any, name: str) -> str:
    identity = _required_text(value, name)
    if AUTOMATION_IDENTITY_PATTERN.search(identity):
        raise PostPilotClosureError(
            f"{name} must identify a human reviewer, not automation."
        )
    return identity


def _utc_iso(value: Any, name: str) -> str:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PostPilotClosureError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PostPilotClosureError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC").isoformat()


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_iso(value, "timestamp")
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_bytes(document: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            _canonical(document),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def _digest(document: dict[str, Any], *, exclude: Iterable[str] = ()) -> str:
    excluded = set(exclude)
    material = {
        key: value for key, value in document.items() if key not in excluded
    }
    return hashlib.sha256(_json_bytes(material)).hexdigest()


def _bytes_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise PostPilotClosureError(f"{name} must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PostPilotClosureError(f"{name} must be a positive integer.") from exc
    if parsed < 1:
        raise PostPilotClosureError(f"{name} must be a positive integer.")
    return parsed


def _safe_relative(value: Any, name: str) -> Path:
    text = _required_text(value, name).replace("\\", "/")
    relative = Path(text)
    if relative.is_absolute() or ".." in relative.parts or "." in relative.parts:
        raise PostPilotClosureError(f"{name} must be a safe relative path.")
    return relative


def _safe_archive_path(value: Any) -> str:
    relative = _safe_relative(value, "archive_path")
    return relative.as_posix()


def _regular_root(path: Path, name: str) -> Path:
    root = Path(path)
    if root.is_symlink() or not root.is_dir():
        raise PostPilotClosureError(
            f"{name} must be a regular non-symlink directory."
        )
    return root.resolve()


def _regular_file_under(root: Path, relative: Path) -> Path:
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise PostPilotClosureError(
                f"Evidence path contains a symbolic link: {relative.as_posix()}."
            )
    try:
        resolved = current.resolve(strict=True)
    except FileNotFoundError as exc:
        raise PostPilotClosureError(
            f"Evidence file is missing: {relative.as_posix()}."
        ) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PostPilotClosureError(
            f"Evidence file escapes evidence_root: {relative.as_posix()}."
        ) from exc
    if not resolved.is_file():
        raise PostPilotClosureError(
            f"Evidence path is not a regular file: {relative.as_posix()}."
        )
    return resolved


def _verify_chain(
    plan: dict[str, Any],
    preflight: dict[str, Any],
    authorization: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
    decision: dict[str, Any],
) -> None:
    try:
        verify_fabric_pilot_plan(plan)
    except FabricPilotError as exc:
        raise PostPilotClosureError(
            f"Pilot plan failed verification: {exc}"
        ) from exc
    try:
        verify_fabric_pilot_preflight(preflight)
        verify_fabric_pilot_authorization(authorization)
    except FabricPilotAuthorizationError as exc:
        raise PostPilotClosureError(
            f"Pilot preflight or authorization failed verification: {exc}"
        ) from exc
    try:
        verify_fabric_pilot_run_receipt(receipt)
        verify_fabric_pilot_run_assessment(assessment)
    except FabricPilotReceiptError as exc:
        raise PostPilotClosureError(
            f"Pilot receipt or assessment failed verification: {exc}"
        ) from exc
    try:
        verify_post_pilot_decision(decision)
    except PostPilotDecisionError as exc:
        raise PostPilotClosureError(
            f"Post-pilot decision failed verification: {exc}"
        ) from exc

    pilot_id = plan.get("pilot_id")
    plan_hash = plan.get("plan_hash")
    if preflight.get("pilot_id") != pilot_id or preflight.get(
        "plan_hash"
    ) != plan_hash:
        raise PostPilotClosureError("Preflight does not match the pilot plan.")
    if authorization.get("pilot_id") != pilot_id or authorization.get(
        "plan_hash"
    ) != plan_hash:
        raise PostPilotClosureError("Authorization does not match the pilot plan.")
    if receipt.get("pilot_id") != pilot_id or receipt.get("plan_hash") != plan_hash:
        raise PostPilotClosureError("Receipt does not match the pilot plan.")
    if assessment.get("pilot_id") != pilot_id:
        raise PostPilotClosureError("Assessment does not match the pilot plan.")
    if decision.get("pilot_id") != pilot_id or decision.get("plan_hash") != plan_hash:
        raise PostPilotClosureError("Decision does not match the pilot plan.")

    if authorization.get("preflight_id") != preflight.get(
        "preflight_id"
    ) or authorization.get("preflight_hash") != preflight.get("preflight_hash"):
        raise PostPilotClosureError("Authorization does not match the preflight.")
    if receipt.get("authorization_id") != authorization.get(
        "authorization_id"
    ) or receipt.get("authorization_hash") != authorization.get(
        "authorization_hash"
    ):
        raise PostPilotClosureError("Receipt does not match the authorization.")
    if assessment.get("receipt_id") != receipt.get(
        "receipt_id"
    ) or assessment.get("receipt_hash") != receipt.get("receipt_hash"):
        raise PostPilotClosureError("Assessment does not match the receipt.")
    if decision.get("authorization_id") != authorization.get(
        "authorization_id"
    ) or decision.get("authorization_hash") != authorization.get(
        "authorization_hash"
    ):
        raise PostPilotClosureError("Decision does not match the authorization.")
    if decision.get("receipt_id") != receipt.get(
        "receipt_id"
    ) or decision.get("receipt_hash") != receipt.get("receipt_hash"):
        raise PostPilotClosureError("Decision does not match the receipt.")
    if decision.get("assessment_id") != assessment.get(
        "assessment_id"
    ) or decision.get("assessment_hash") != assessment.get("assessment_hash"):
        raise PostPilotClosureError("Decision does not match the assessment.")
    if decision.get("external_run_id") != receipt.get("external_run_id"):
        raise PostPilotClosureError("Decision does not match the external run.")
    if decision.get("candidate_id") != plan.get(
        "candidate_id"
    ) or decision.get("candidate_version") != plan.get("candidate_version"):
        raise PostPilotClosureError("Decision does not match the candidate.")


def verify_post_pilot_closure_manifest(manifest: dict[str, Any]) -> None:
    """Verify manifest identity, item accounting, hash, and safety flags."""
    if not isinstance(manifest, dict):
        raise PostPilotClosureError("Closure manifest must be a JSON object.")
    if manifest.get("closure_manifest_contract_version") != MANIFEST_CONTRACT_VERSION:
        raise PostPilotClosureError("Unsupported closure manifest contract.")
    if manifest.get("manifest_hash") != _digest(
        manifest, exclude=("manifest_hash",)
    ):
        raise PostPilotClosureError("Closure manifest hash is invalid.")
    closure_id = _required_text(manifest.get("closure_id"), "closure_id")
    if not CLOSURE_ID_PATTERN.fullmatch(closure_id):
        raise PostPilotClosureError("Closure ID is malformed.")
    if manifest.get("closure_revision") != 1 or manifest.get(
        "closure_state"
    ) != "closed":
        raise PostPilotClosureError("Closure revision or state is invalid.")
    _human_identity(manifest.get("created_by"), "created_by")
    _required_text(manifest.get("review_ticket"), "review_ticket")
    reason = _required_text(manifest.get("reason"), "reason")
    if len(reason) < 20:
        raise PostPilotClosureError("reason must contain at least 20 characters.")
    _utc_iso(manifest.get("created_at_utc"), "created_at_utc")

    items = manifest.get("items")
    if not isinstance(items, list) or len(items) < 7:
        raise PostPilotClosureError(
            "Closure manifest must contain six documents and run evidence."
        )
    paths: set[str] = set()
    total = 0
    for sequence, item in enumerate(items, start=1):
        if not isinstance(item, dict) or item.get("item_sequence") != sequence:
            raise PostPilotClosureError(
                "Closure manifest item sequence is invalid."
            )
        archive_path = _safe_archive_path(item.get("archive_path"))
        if archive_path == MANIFEST_ARCHIVE_PATH:
            raise PostPilotClosureError(
                "Manifest cannot declare itself as a closure item."
            )
        if archive_path in paths:
            raise PostPilotClosureError(
                f"Closure manifest contains duplicate path {archive_path}."
            )
        paths.add(archive_path)
        _required_text(item.get("item_role"), "item_role")
        size = item.get("size_bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise PostPilotClosureError("Closure item size is invalid.")
        total += size
        if not SHA256_PATTERN.fullmatch(
            _required_text(item.get("sha256"), "item sha256")
        ):
            raise PostPilotClosureError("Closure item SHA-256 is malformed.")
        source_relative = item.get("source_relative_path")
        if source_relative is not None:
            _safe_relative(source_relative, "source_relative_path")
    if manifest.get("item_count") != len(items):
        raise PostPilotClosureError("Closure item count is inconsistent.")
    if manifest.get("total_size_bytes") != total:
        raise PostPilotClosureError("Closure total size is inconsistent.")

    safety = {
        "active_model_unchanged": True,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
    }
    for field, expected in safety.items():
        if manifest.get(field) is not expected:
            raise PostPilotClosureError(
                f"Closure safety flag {field} is invalid."
            )


def _document_items(
    documents: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, bytes]]:
    items: list[dict[str, Any]] = []
    payloads: dict[str, bytes] = {}
    for role, archive_path in DOCUMENT_ROLES:
        payload = _json_bytes(documents[role])
        payloads[archive_path] = payload
        items.append(
            {
                "item_role": role,
                "archive_path": archive_path,
                "source_relative_path": None,
                "size_bytes": len(payload),
                "sha256": _bytes_sha256(payload),
            }
        )
    return items, payloads


def _evidence_items(
    receipt: dict[str, Any], evidence_root: Path
) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    root = _regular_root(evidence_root, "evidence_root")
    receipt_items = receipt.get("evidence_files")
    if not isinstance(receipt_items, list) or not receipt_items:
        raise PostPilotClosureError("Receipt must reference run evidence files.")
    items: list[dict[str, Any]] = []
    sources: dict[str, Path] = {}
    for expected_sequence, receipt_item in enumerate(receipt_items, start=1):
        if receipt_item.get("evidence_sequence") != expected_sequence:
            raise PostPilotClosureError("Receipt evidence sequence is invalid.")
        relative = _safe_relative(
            receipt_item.get("relative_path"), "receipt evidence path"
        )
        source = _regular_file_under(root, relative)
        expected_size = receipt_item.get("size_bytes")
        expected_sha = _required_text(
            receipt_item.get("sha256"), "receipt evidence sha256"
        )
        actual_size = source.stat().st_size
        actual_sha = _file_sha256(source)
        if actual_size != expected_size or actual_sha != expected_sha:
            raise PostPilotClosureError(
                f"Run evidence no longer matches the receipt: {relative.as_posix()}."
            )
        archive_path = (
            f"evidence/{expected_sequence:04d}/{relative.as_posix()}"
        )
        archive_path = _safe_archive_path(archive_path)
        role = _required_text(receipt_item.get("evidence_role"), "evidence_role")
        items.append(
            {
                "item_role": f"run_evidence:{role}",
                "archive_path": archive_path,
                "source_relative_path": relative.as_posix(),
                "size_bytes": actual_size,
                "sha256": actual_sha,
            }
        )
        sources[archive_path] = source
    return items, sources


def _tar_info(name: str, size: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = size
    info.mode = 0o600
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info


def create_post_pilot_closure_bundle(
    plan: dict[str, Any],
    preflight: dict[str, Any],
    authorization: dict[str, Any],
    receipt: dict[str, Any],
    assessment: dict[str, Any],
    decision: dict[str, Any],
    evidence_root: Path,
    output_root: Path,
    *,
    created_by: str,
    review_ticket: str,
    reason: str,
    created_at_utc: Any | None = None,
    max_members: int = DEFAULT_MAX_MEMBERS,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Create and immediately verify one immutable deterministic closure tar."""
    _verify_chain(plan, preflight, authorization, receipt, assessment, decision)
    creator = _human_identity(created_by, "created_by")
    ticket = _required_text(review_ticket, "review_ticket")
    rationale = _required_text(reason, "reason")
    if len(rationale) < 20:
        raise PostPilotClosureError("reason must contain at least 20 characters.")
    created_at = _utc_iso(
        created_at_utc or datetime.now(timezone.utc), "created_at_utc"
    )
    max_members = _positive_int(max_members, "max_members")
    max_total_bytes = _positive_int(max_total_bytes, "max_total_bytes")

    documents = {
        "pilot_plan": plan,
        "pilot_preflight": preflight,
        "pilot_authorization": authorization,
        "pilot_run_receipt": receipt,
        "pilot_run_assessment": assessment,
        "post_pilot_decision": decision,
    }
    document_items, document_payloads = _document_items(documents)
    evidence_items, evidence_sources = _evidence_items(receipt, evidence_root)
    unordered_items = [*document_items, *evidence_items]
    unordered_items.sort(key=lambda item: item["archive_path"])
    items = [
        {"item_sequence": sequence, **item}
        for sequence, item in enumerate(unordered_items, start=1)
    ]
    total_size = sum(item["size_bytes"] for item in items)
    if len(items) > max_members:
        raise PostPilotClosureError(
            f"Closure item count exceeds max_members={max_members}."
        )
    if total_size > max_total_bytes:
        raise PostPilotClosureError(
            f"Closure evidence exceeds max_total_bytes={max_total_bytes}."
        )

    identity_core = {
        "pilot_id": plan["pilot_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_id": plan["candidate_id"],
        "candidate_version": plan["candidate_version"],
        "authorization_id": authorization["authorization_id"],
        "authorization_hash": authorization["authorization_hash"],
        "receipt_id": receipt["receipt_id"],
        "receipt_hash": receipt["receipt_hash"],
        "assessment_id": assessment["assessment_id"],
        "assessment_hash": assessment["assessment_hash"],
        "decision_id": decision["decision_id"],
        "decision_hash": decision["decision_hash"],
        "decision": decision["decision"],
        "external_run_id": receipt["external_run_id"],
        "created_by": creator,
        "review_ticket": ticket,
        "reason": rationale,
        "created_at_utc": created_at,
        "items": items,
    }
    closure_id = "pcl-" + _digest(identity_core)[:24]
    manifest = {
        "closure_id": closure_id,
        "closure_revision": 1,
        "closure_state": "closed",
        **identity_core,
        "item_count": len(items),
        "total_size_bytes": total_size,
        "active_model_unchanged": True,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "schedule_activation_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
        "closure_manifest_contract_version": MANIFEST_CONTRACT_VERSION,
    }
    manifest["manifest_hash"] = _digest(manifest, exclude=("manifest_hash",))
    verify_post_pilot_closure_manifest(manifest)

    output_directory = Path(output_root) / plan["pilot_id"]
    bundle_path = output_directory / f"post_pilot_closure_{closure_id}.tar"
    temporary = bundle_path.with_suffix(".tmp")
    for candidate in (bundle_path, temporary):
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    output_directory.mkdir(parents=True, exist_ok=True)
    manifest_payload = _json_bytes(manifest)
    payload_lookup = dict(document_payloads)
    try:
        with tarfile.open(temporary, mode="w", format=tarfile.PAX_FORMAT) as archive:
            members = [MANIFEST_ARCHIVE_PATH, *[item["archive_path"] for item in items]]
            for archive_path in sorted(members):
                if archive_path == MANIFEST_ARCHIVE_PATH:
                    payload = manifest_payload
                    archive.addfile(
                        _tar_info(archive_path, len(payload)), io.BytesIO(payload)
                    )
                elif archive_path in payload_lookup:
                    payload = payload_lookup[archive_path]
                    archive.addfile(
                        _tar_info(archive_path, len(payload)), io.BytesIO(payload)
                    )
                else:
                    source = evidence_sources[archive_path]
                    with source.open("rb") as handle:
                        archive.addfile(
                            _tar_info(archive_path, source.stat().st_size), handle
                        )
        temporary.replace(bundle_path)
    finally:
        temporary.unlink(missing_ok=True)

    verified_manifest, verification = verify_post_pilot_closure_bundle(
        bundle_path,
        max_members=max_members,
        max_total_bytes=max_total_bytes,
    )
    if verified_manifest != manifest:
        raise PostPilotClosureError(
            "Closure archive manifest changed during creation."
        )
    return bundle_path, manifest, verification


def _verification_record(
    manifest: dict[str, Any],
    *,
    kind: str,
    verified_at_utc: Any | None = None,
    archive_sha256: str | None = None,
    recovered_directory: str | None = None,
) -> dict[str, Any]:
    if kind not in {"archive", "recovery"}:
        raise PostPilotClosureError("Verification kind is unsupported.")
    verified_at = _utc_iso(
        verified_at_utc or datetime.now(timezone.utc), "verified_at_utc"
    )
    core = {
        "verification_kind": kind,
        "closure_id": manifest["closure_id"],
        "pilot_id": manifest["pilot_id"],
        "manifest_hash": manifest["manifest_hash"],
        "archive_sha256": archive_sha256,
        "recovered_directory": recovered_directory,
        "verification_status": "verified",
        "verified_at_utc": verified_at,
        "item_count": manifest["item_count"],
        "total_size_bytes": manifest["total_size_bytes"],
    }
    verification = {
        "verification_id": "pcv-" + _digest(core)[:24],
        **core,
        "active_model_unchanged": True,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
        "closure_verification_contract_version": VERIFICATION_CONTRACT_VERSION,
    }
    verification["verification_hash"] = _digest(
        verification, exclude=("verification_hash",)
    )
    verify_post_pilot_closure_verification(verification)
    return verification


def verify_post_pilot_closure_verification(
    verification: dict[str, Any]
) -> None:
    if not isinstance(verification, dict):
        raise PostPilotClosureError("Closure verification must be a JSON object.")
    if verification.get(
        "closure_verification_contract_version"
    ) != VERIFICATION_CONTRACT_VERSION:
        raise PostPilotClosureError("Unsupported closure verification contract.")
    if verification.get("verification_hash") != _digest(
        verification, exclude=("verification_hash",)
    ):
        raise PostPilotClosureError("Closure verification hash is invalid.")
    if not VERIFICATION_ID_PATTERN.fullmatch(
        _required_text(verification.get("verification_id"), "verification_id")
    ):
        raise PostPilotClosureError("Verification ID is malformed.")
    if verification.get("verification_kind") not in {"archive", "recovery"}:
        raise PostPilotClosureError("Verification kind is unsupported.")
    if verification.get("verification_status") != "verified":
        raise PostPilotClosureError("Closure verification status is invalid.")
    _utc_iso(verification.get("verified_at_utc"), "verified_at_utc")
    if verification.get("verification_kind") == "archive":
        if not SHA256_PATTERN.fullmatch(
            _required_text(verification.get("archive_sha256"), "archive_sha256")
        ):
            raise PostPilotClosureError("Archive SHA-256 is malformed.")
    safety = {
        "active_model_unchanged": True,
        "model_registry_mutation_allowed": False,
        "authorization_reuse_allowed": False,
        "deployment_authorized": False,
        "model_activation_authorized": False,
        "source_evidence_mutated": False,
        "permanent_deletion_allowed": False,
    }
    for field, expected in safety.items():
        if verification.get(field) is not expected:
            raise PostPilotClosureError(
                f"Closure verification safety flag {field} is invalid."
            )


def verify_post_pilot_closure_bundle(
    bundle_path: Path,
    *,
    max_members: int = DEFAULT_MAX_MEMBERS,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_manifest_bytes: int = DEFAULT_MAX_MANIFEST_BYTES,
    verified_at_utc: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify archive safety, manifest accounting, and every declared hash."""
    source = Path(bundle_path)
    if source.is_symlink() or not source.is_file():
        raise PostPilotClosureError(
            "Closure bundle path must be a regular non-symlink file."
        )
    max_members = _positive_int(max_members, "max_members")
    max_total_bytes = _positive_int(max_total_bytes, "max_total_bytes")
    max_manifest_bytes = _positive_int(max_manifest_bytes, "max_manifest_bytes")
    try:
        archive = tarfile.open(source, mode="r:*")
    except (tarfile.TarError, OSError) as exc:
        raise PostPilotClosureError("Closure bundle is not a readable tar archive.") from exc
    with archive:
        members = archive.getmembers()
        if len(members) > max_members + 1:
            raise PostPilotClosureError("Closure archive contains too many members.")
        names: set[str] = set()
        total_member_size = 0
        for member in members:
            name = _safe_archive_path(member.name)
            if name in names:
                raise PostPilotClosureError(
                    f"Closure archive contains duplicate member {name}."
                )
            names.add(name)
            if not member.isfile():
                raise PostPilotClosureError(
                    f"Closure archive member is not a regular file: {name}."
                )
            total_member_size += member.size
            if total_member_size > max_total_bytes + max_manifest_bytes:
                raise PostPilotClosureError(
                    "Closure archive exceeds the configured byte bound."
                )
        if MANIFEST_ARCHIVE_PATH not in names:
            raise PostPilotClosureError("Closure archive is missing its manifest.")
        manifest_member = archive.getmember(MANIFEST_ARCHIVE_PATH)
        if manifest_member.size > max_manifest_bytes:
            raise PostPilotClosureError("Closure manifest exceeds its size bound.")
        manifest_handle = archive.extractfile(manifest_member)
        if manifest_handle is None:
            raise PostPilotClosureError("Closure manifest cannot be read.")
        try:
            manifest = json.loads(manifest_handle.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PostPilotClosureError("Closure manifest is invalid JSON.") from exc
        verify_post_pilot_closure_manifest(manifest)
        if manifest["item_count"] > max_members:
            raise PostPilotClosureError("Manifest item count exceeds the member bound.")
        if manifest["total_size_bytes"] > max_total_bytes:
            raise PostPilotClosureError("Manifest byte total exceeds the byte bound.")
        expected = {
            MANIFEST_ARCHIVE_PATH,
            *[item["archive_path"] for item in manifest["items"]],
        }
        missing = sorted(expected - names)
        unexpected = sorted(names - expected)
        if missing or unexpected:
            raise PostPilotClosureError(
                "Closure archive membership does not match its manifest; "
                f"missing={missing}, unexpected={unexpected}."
            )
        for item in manifest["items"]:
            member = archive.getmember(item["archive_path"])
            if member.size != item["size_bytes"]:
                raise PostPilotClosureError(
                    f"Closure member size mismatch: {item['archive_path']}."
                )
            handle = archive.extractfile(member)
            if handle is None:
                raise PostPilotClosureError(
                    f"Closure member cannot be read: {item['archive_path']}."
                )
            digest = hashlib.sha256()
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != item["sha256"]:
                raise PostPilotClosureError(
                    f"Closure member hash mismatch: {item['archive_path']}."
                )
    verification = _verification_record(
        manifest,
        kind="archive",
        verified_at_utc=verified_at_utc,
        archive_sha256=_file_sha256(source),
    )
    return manifest, verification


def verify_recovered_post_pilot_closure(
    recovered_directory: Path,
    *,
    verified_at_utc: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify an extracted closure directory contains exactly the manifest set."""
    root = _regular_root(recovered_directory, "recovered_directory")
    manifest_path = root / MANIFEST_ARCHIVE_PATH
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise PostPilotClosureError("Recovered closure is missing a regular manifest.")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PostPilotClosureError("Recovered closure manifest is invalid JSON.") from exc
    verify_post_pilot_closure_manifest(manifest)
    expected = {
        MANIFEST_ARCHIVE_PATH,
        *[item["archive_path"] for item in manifest["items"]],
    }
    actual: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise PostPilotClosureError(
                f"Recovered closure contains a symbolic link: {path}."
            )
        if path.is_dir():
            continue
        if not path.is_file():
            raise PostPilotClosureError(
                f"Recovered closure contains a non-regular entry: {path}."
            )
        actual.add(path.relative_to(root).as_posix())
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        raise PostPilotClosureError(
            "Recovered closure membership does not match its manifest; "
            f"missing={missing}, unexpected={unexpected}."
        )
    for item in manifest["items"]:
        path = root / _safe_relative(item["archive_path"], "archive_path")
        if path.stat().st_size != item["size_bytes"] or _file_sha256(path) != item[
            "sha256"
        ]:
            raise PostPilotClosureError(
                f"Recovered closure file does not match its manifest: {item['archive_path']}."
            )
    verification = _verification_record(
        manifest,
        kind="recovery",
        verified_at_utc=verified_at_utc,
        recovered_directory=str(root),
    )
    return manifest, verification


def recover_post_pilot_closure_bundle(
    bundle_path: Path,
    recovery_root: Path,
    *,
    max_members: int = DEFAULT_MAX_MEMBERS,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    verified_at_utc: Any | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Safely extract a verified closure into one new directory and re-verify it."""
    manifest, _ = verify_post_pilot_closure_bundle(
        bundle_path,
        max_members=max_members,
        max_total_bytes=max_total_bytes,
        verified_at_utc=verified_at_utc,
    )
    root = Path(recovery_root)
    if root.is_symlink():
        raise PostPilotClosureError("recovery_root must not be a symbolic link.")
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir():
        raise PostPilotClosureError("recovery_root must be a directory.")
    target = root / manifest["closure_id"]
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"Refusing to overwrite {target}.")
    target.mkdir()
    try:
        with tarfile.open(bundle_path, mode="r:*") as archive:
            for member in archive.getmembers():
                archive_path = _safe_relative(member.name, "archive_path")
                if not member.isfile():
                    raise PostPilotClosureError(
                        f"Closure archive member is not regular: {member.name}."
                    )
                destination = target / archive_path
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() or destination.is_symlink():
                    raise FileExistsError(f"Refusing to overwrite {destination}.")
                handle = archive.extractfile(member)
                if handle is None:
                    raise PostPilotClosureError(
                        f"Closure member cannot be read: {member.name}."
                    )
                with destination.open("xb") as output:
                    shutil.copyfileobj(handle, output, length=1024 * 1024)
        _, recovery_verification = verify_recovered_post_pilot_closure(
            target, verified_at_utc=verified_at_utc
        )
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise
    return target, recovery_verification


def _write_json_exclusive(path: Path, document: dict[str, Any]) -> Path:
    temporary = path.with_suffix(".tmp")
    for candidate in (path, temporary):
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(_canonical(document), handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def write_post_pilot_closure_verification(
    output_root: Path, verification: dict[str, Any]
) -> Path:
    verify_post_pilot_closure_verification(verification)
    path = (
        Path(output_root)
        / verification["pilot_id"]
        / (
            f"post_pilot_closure_{verification['closure_id']}_"
            f"{verification['verification_kind']}_verification_"
            f"{verification['verification_id']}.json"
        )
    )
    return _write_json_exclusive(path, verification)


def load_post_pilot_closure_manifest_from_bundle(
    bundle_path: Path,
) -> dict[str, Any]:
    manifest, _ = verify_post_pilot_closure_bundle(bundle_path)
    return deepcopy(manifest)
