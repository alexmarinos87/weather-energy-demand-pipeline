"""No-replace publication for the scenario adapter's four-file evidence bundle."""
from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

from forecasting import interval_policy_retained_compatibility_manifest as manifests
from forecasting._interval_policy_retained_compatibility_common import (
    IntervalPolicyRetainedCompatibilityError,
    prepare_compatibility_summary,
)


def _require_absent(paths: dict[str, Path]) -> None:
    for path in paths.values():
        try:
            path.lstat()  # Unlike exists(), this also detects a dangling symlink.
        except FileNotFoundError:
            continue
        raise FileExistsError(f"Refusing to overwrite existing evidence: {path}")


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    with path.open("xb") as handle:
        if output_format == "csv":
            frame.to_csv(handle, index=False, encoding="utf-8")
        else:
            frame.to_parquet(handle, index=False)


def _rollback(owned: dict[Path, tuple[int, int]], error: BaseException) -> None:
    """Remove our inodes only; never delete a replacement from another writer."""
    for path, identity in reversed(list(owned.items())):
        try:
            current = path.lstat()
            if (current.st_dev, current.st_ino) == identity:
                path.unlink()
        except FileNotFoundError:
            pass
        except OSError as cleanup_error:
            # Attempt remaining cleanup without masking the publication failure.
            error.add_note(f"Could not roll back {path}: {cleanup_error}")


def write_retained_compatibility_bundle(
    output_directory: str | Path,
    slices: pd.DataFrame,
    summary: pd.DataFrame,
    report: str,
    *,
    output_format: str = "csv",
) -> dict[str, Path]:
    """Stage, verify, then publish evaluator-produced v2 evidence without replacement.

    The manifest is published last. Ordinary failures roll back this invocation's
    files when the filesystem permits cleanup. Hard-link support is required.
    This is not a crash-atomic multi-file transaction or a hostile-directory lock.
    """
    if output_format not in {"csv", "parquet"}:
        raise IntervalPolicyRetainedCompatibilityError(
            "output_format must be csv or parquet."
        )
    prepared = prepare_compatibility_summary(summary)
    run_id = str(prepared["compatibility_run_id"].iloc[0])
    if (
        slices.empty
        or "compatibility_run_id" not in slices
        or slices["compatibility_run_id"].isna().any()
        or not slices["compatibility_run_id"].eq(run_id).all()
    ):
        raise IntervalPolicyRetainedCompatibilityError(
            "Non-empty slices must bind the same compatibility_run_id as the summary."
        )
    if not isinstance(report, str) or not report.strip():
        raise IntervalPolicyRetainedCompatibilityError("The compatibility report must not be empty.")

    root = Path(output_directory)
    # Insertion order is intentional: the already verified manifest is last.
    outputs = {
        role: root / (
            f"interval_policy_retained_compatibility_{role}_{run_id}."
            + {"report": "md", "manifest": "json"}.get(role, output_format)
        )
        for role in ("slices", "summary", "report", "manifest")
    }
    _require_absent(outputs)
    root.mkdir(parents=True, exist_ok=True)
    owned: dict[Path, tuple[int, int]] = {}
    try:
        # Staging on the same filesystem allows no-replace hard-link publication.
        with TemporaryDirectory(prefix=f".{run_id}-stage-", dir=root) as temporary:
            staged = {role: Path(temporary) / path.name for role, path in outputs.items()}
            _write_frame(slices, staged["slices"], output_format)
            _write_frame(summary, staged["summary"], output_format)
            with staged["report"].open("x", encoding="utf-8") as handle:
                handle.write(report)
            # The builder also verifies exact policy, artifact and saved-summary bindings.
            manifest: dict[str, Any] = manifests.build_compatibility_manifest(
                prepared, artifacts={role: staged[role] for role in manifests.ARTIFACT_ROLES}
            )
            with staged["manifest"].open("x", encoding="utf-8") as handle:
                json.dump(manifest, handle, indent=2, sort_keys=True)
                handle.write("\n")
            for role, destination in outputs.items():
                source = staged[role]
                status = source.stat()
                # Record before linking: an interrupt after link creation still rolls back.
                owned[destination] = (status.st_dev, status.st_ino)
                try:
                    destination.hardlink_to(source)
                except FileExistsError as exc:
                    raise FileExistsError(
                        f"Refusing to overwrite existing evidence: {destination}"
                    ) from exc
    except BaseException as error:
        _rollback(owned, error)
        raise
    return outputs
