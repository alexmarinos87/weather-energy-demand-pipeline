"""Hardened public API for reproducible evidence bundles.

The implementation lives in :mod:`forecasting._evidence_bundle_impl`. This
facade preserves the established public API while performing lexical,
symlink-aware source preflight before delegation. An absolute source path must
not lose its link identity through an early ``resolve()``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from . import _evidence_bundle_impl as _impl


EvidenceBundleError = _impl.EvidenceBundleError
verify_evidence_bundle = _impl.verify_evidence_bundle
recover_evidence_bundle = _impl.recover_evidence_bundle
verify_recovered_bundle = _impl.verify_recovered_bundle


def _lexical_relative_under(root: Path, value: Any, name: str) -> Path:
    """Return a safe lexical path below ``root`` without following links."""
    path = Path(value)
    if path.is_absolute():
        try:
            relative = path.relative_to(root)
        except ValueError as exc:
            raise EvidenceBundleError(
                f"{name} is outside the data root: {path}."
            ) from exc
        relative = _impl._safe_relative(relative, name)
    else:
        relative = _impl._safe_relative(path, name)
    _impl._reject_symlink_components(root, relative, name)
    return relative


def _preflight_file(root: Path, value: Any, name: str) -> None:
    relative = _lexical_relative_under(root, value, name)
    candidate = root / relative
    if candidate.is_symlink():
        raise EvidenceBundleError(
            f"{name} must be a regular non-symlink file."
        )
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise EvidenceBundleError(f"{name} does not exist: {relative}.") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvidenceBundleError(f"{name} escapes the data root.") from exc
    if not resolved.is_file():
        raise EvidenceBundleError(
            f"{name} must be a regular non-symlink file."
        )


def _preflight_directory(root: Path, value: Any, name: str) -> None:
    relative = _lexical_relative_under(root, value, name)
    candidate = root / relative
    if candidate.is_symlink():
        raise EvidenceBundleError(
            f"{name} must be a regular non-symlink directory."
        )
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise EvidenceBundleError(f"{name} does not exist: {relative}.") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvidenceBundleError(f"{name} escapes the data root.") from exc
    if not resolved.is_dir():
        raise EvidenceBundleError(
            f"{name} must be a regular non-symlink directory."
        )


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
):
    """Create a bundle only after every source passes link-safe preflight."""
    root = _impl._root(data_root, "data_root")
    roles = dict(role_paths)
    extras = tuple(extra_paths)

    _preflight_directory(root, candidate_dir, "candidate_dir")
    for role, path in roles.items():
        _preflight_file(root, path, f"{role}_path")
    for index, path in enumerate(extras, start=1):
        _preflight_file(root, path, f"extra_path[{index}]")

    return _impl.create_evidence_bundle(
        data_root,
        candidate_dir,
        roles,
        extra_paths=extras,
        actor=actor,
        reason=reason,
        created_at_utc=created_at_utc,
        output_root=output_root,
        max_bundle_bytes=max_bundle_bytes,
    )


__all__ = [
    "EvidenceBundleError",
    "create_evidence_bundle",
    "recover_evidence_bundle",
    "verify_evidence_bundle",
    "verify_recovered_bundle",
]
