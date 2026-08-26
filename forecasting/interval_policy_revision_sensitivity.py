from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from forecasting._interval_policy_revision_sensitivity_common import (
    REVISION_SENSITIVITY_CONTRACT_VERSION,
    REVISION_SENSITIVITY_SAFETY_FIELDS,
    IntervalPolicyRevisionSensitivityError,
    canonical,
    digest,
    file_sha256,
    utc_timestamp,
)
from forecasting.interval_policy_candidate_revision import (
    verify_candidate_revision_package,
)
from forecasting.interval_policy_candidate_revision_review import (
    verify_candidate_revision_review,
)
from forecasting.interval_policy_sensitivity import (
    PolicyCandidate,
    default_policy_candidates,
    evaluate_policy_sensitivity,
    prepare_policy_sensitivity_input,
    read_frame,
    write_frame_atomic,
    write_text_atomic,
)


BINDING_COLUMNS = (
    "source_revision_review_id",
    "source_revision_review_sha256",
    "source_revision_package_id",
    "source_revision_package_sha256",
    "source_decision_id",
    "source_decision_sha256",
    "revised_candidate_id",
    "revised_candidate_version",
    "revised_candidate_sha256",
    "revision_sensitivity_contract_version",
    *REVISION_SENSITIVITY_SAFETY_FIELDS,
)


def _revised_policy_candidate(package: Mapping[str, Any]) -> PolicyCandidate:
    revised = package["revised_candidate"]
    candidate = PolicyCandidate(
        candidate_id=revised["candidate_id"],
        candidate_role=revised["candidate_role"],
        candidate_version=revised["candidate_version"],
        rationale=revised["rationale"],
        min_recent_interval_runs=int(revised["min_recent_interval_runs"]),
        min_reference_interval_runs=int(
            revised["min_reference_interval_runs"]
        ),
        max_interval_run_age_minutes=int(
            revised["max_interval_run_age_minutes"]
        ),
        max_evaluation_age_minutes=int(
            revised["max_evaluation_age_minutes"]
        ),
        min_calibration_observation_count=int(
            revised["min_calibration_observation_count"]
        ),
        max_recent_coverage_shortfall_pct_points=float(
            revised["max_recent_coverage_shortfall_pct_points"]
        ),
        max_coverage_drop_pct_points=float(
            revised["max_coverage_drop_pct_points"]
        ),
        max_average_interval_width_increase_pct=float(
            revised["max_average_interval_width_increase_pct"]
        ),
        max_calibration_history_drop_pct=float(
            revised["max_calibration_history_drop_pct"]
        ),
        source_policy_version=revised["policy_version"],
    )
    candidate.validate()
    return candidate


def run_revision_sensitivity(
    slice_trends: pd.DataFrame,
    review: Mapping[str, Any],
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
    *,
    sensitivity_run_id: str | None = None,
    sensitivity_run_timestamp: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Run one explicit revised-candidate comparison over retained trends."""
    review_document = dict(review)
    package_document = dict(package)
    decision_document = dict(decision)
    verify_candidate_revision_review(
        review_document,
        package_document,
        decision_document,
        sensitivity_summary,
    )
    if review_document.get("review_decision") != (
        "accept_for_sensitivity_review"
    ):
        raise IntervalPolicyRevisionSensitivityError(
            "A revision sensitivity run requires accept_for_sensitivity_review."
        )
    if review_document.get("next_action") != (
        "separate_sensitivity_review_request_required"
    ):
        raise IntervalPolicyRevisionSensitivityError(
            "The source review does not require a separate sensitivity review."
        )
    trends = prepare_policy_sensitivity_input(slice_trends)
    retained_trend_id = trends["trend_run_id"].iloc[0]
    if retained_trend_id != review_document.get("trend_run_id"):
        raise IntervalPolicyRevisionSensitivityError(
            "Slice trends do not match the reviewed evidence chain."
        )
    run_timestamp = (
        sensitivity_run_timestamp
        if sensitivity_run_timestamp is not None
        else pd.Timestamp.now(tz="UTC")
    )
    timestamp = utc_timestamp(
        run_timestamp, "sensitivity_run_timestamp"
    )
    reviewed_at = utc_timestamp(
        review_document["reviewed_at_utc"], "reviewed_at_utc"
    )
    if timestamp < reviewed_at:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity run cannot precede the named review."
        )
    unsupported_window_fields = {
        item["field"]
        for item in package_document["threshold_changes"]
        if item["field"] in {
            "recent_interval_run_count",
            "reference_interval_run_count",
        }
    }
    if unsupported_window_fields:
        raise IntervalPolicyRevisionSensitivityError(
            "Retained slice trends cannot evaluate revised monitoring window "
            "geometry: " + ", ".join(sorted(unsupported_window_fields)) + "."
        )
    active = default_policy_candidates()[0]
    revised = _revised_policy_candidate(package_document)
    slices, summary, base_report = evaluate_policy_sensitivity(
        trends,
        candidates=(active, revised),
        sensitivity_run_id=sensitivity_run_id,
        sensitivity_run_timestamp=timestamp,
    )
    bindings = {
        "source_revision_review_id": review_document["revision_review_id"],
        "source_revision_review_sha256": review_document[
            "revision_review_sha256"
        ],
        "source_revision_package_id": package_document[
            "revision_package_id"
        ],
        "source_revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "revised_candidate_id": package_document["revised_candidate"][
            "candidate_id"
        ],
        "revised_candidate_version": package_document["revised_candidate"][
            "candidate_version"
        ],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
        "revision_sensitivity_contract_version": (
            REVISION_SENSITIVITY_CONTRACT_VERSION
        ),
        **{
            field: False
            for field in REVISION_SENSITIVITY_SAFETY_FIELDS
        },
    }
    for field, value in bindings.items():
        slices[field] = value
        summary[field] = value
    report = "\n".join(
        [
            "# Reviewed candidate revision sensitivity comparison",
            "",
            f"- Source review: `{review_document['revision_review_id']}`",
            f"- Revision package: `{package_document['revision_package_id']}`",
            f"- Revised candidate: `{revised.candidate_id}`",
            "",
            base_report,
            "This comparison is retained evidence only. It does not update "
            "the active policy or activate the revised candidate.",
            "",
        ]
    )
    verify_revision_sensitivity(
        slices,
        summary,
        review_document,
        package_document,
        decision_document,
        sensitivity_summary,
    )
    return slices, summary, report


def verify_revision_sensitivity(
    slices: pd.DataFrame,
    summary: pd.DataFrame,
    review: Mapping[str, Any],
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> None:
    """Verify bindings, candidate scope, and non-activation evidence."""
    if slices.empty or summary.empty:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity outputs must be non-empty."
        )
    review_document = dict(review)
    package_document = dict(package)
    decision_document = dict(decision)
    verify_candidate_revision_review(
        review_document,
        package_document,
        decision_document,
        sensitivity_summary,
    )
    if review_document.get("review_decision") != (
        "accept_for_sensitivity_review"
    ):
        raise IntervalPolicyRevisionSensitivityError(
            "The named review did not accept the package."
        )
    missing_slices = sorted(set(BINDING_COLUMNS) - set(slices.columns))
    missing_summary = sorted(set(BINDING_COLUMNS) - set(summary.columns))
    if missing_slices or missing_summary:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity outputs are missing source bindings."
        )
    expected = {
        "source_revision_review_id": review_document["revision_review_id"],
        "source_revision_review_sha256": review_document[
            "revision_review_sha256"
        ],
        "source_revision_package_id": package_document[
            "revision_package_id"
        ],
        "source_revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "revised_candidate_id": package_document["revised_candidate"][
            "candidate_id"
        ],
        "revised_candidate_version": package_document["revised_candidate"][
            "candidate_version"
        ],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
        "revision_sensitivity_contract_version": (
            REVISION_SENSITIVITY_CONTRACT_VERSION
        ),
    }
    for frame_name, frame in (("slices", slices), ("summary", summary)):
        for field, value in expected.items():
            if set(frame[field].astype(str)) != {str(value)}:
                raise IntervalPolicyRevisionSensitivityError(
                    f"Revision sensitivity {frame_name} {field} is inconsistent."
                )
        for field in REVISION_SENSITIVITY_SAFETY_FIELDS:
            values = frame[field]
            if values.dtype == bool:
                enabled = values.any()
            else:
                enabled = values.astype(str).str.lower().ne("false").any()
            if enabled:
                raise IntervalPolicyRevisionSensitivityError(
                    f"Revision sensitivity safety field {field} must be false."
                )
    revised_id = package_document["revised_candidate"]["candidate_id"]
    expected_candidates = {"active-reference", revised_id}
    if set(slices["candidate_id"]) != expected_candidates:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity slices contain unexpected candidates."
        )
    if set(summary["candidate_id"]) != expected_candidates:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity summary contains unexpected candidates."
        )
    if slices["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity slices must contain one run."
        )
    if summary["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity summary must contain one run."
        )
    if set(slices["trend_run_id"]) != {review_document["trend_run_id"]}:
        raise IntervalPolicyRevisionSensitivityError(
            "Revision sensitivity trend identity is inconsistent."
        )
    active = summary.loc[summary["candidate_id"] == "active-reference"]
    if not (
        active["candidate_status"].to_numpy()
        == active["retained_monitor_status"].to_numpy()
    ).all():
        raise IntervalPolicyRevisionSensitivityError(
            "Active-reference outcomes do not reproduce retained monitoring."
        )


def write_revision_sensitivity(
    output_directory: Path,
    slices: pd.DataFrame,
    summary: pd.DataFrame,
    report: str,
    review: Mapping[str, Any],
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
    *,
    output_format: str = "parquet",
) -> tuple[Path, Path, Path, Path]:
    """Write immutable outputs and a hash-bound manifest."""
    verify_revision_sensitivity(
        slices, summary, review, package, decision, sensitivity_summary
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    run_id = str(summary["sensitivity_run_id"].iloc[0])
    extension = "csv" if output_format == "csv" else "parquet"
    slices_path = output_directory / (
        f"interval_policy_revision_sensitivity_slices_{run_id}.{extension}"
    )
    summary_path = output_directory / (
        f"interval_policy_revision_sensitivity_summary_{run_id}.{extension}"
    )
    report_path = output_directory / (
        f"interval_policy_revision_sensitivity_report_{run_id}.md"
    )
    manifest_path = output_directory / (
        f"interval_policy_revision_sensitivity_manifest_{run_id}.json"
    )
    created: list[Path] = []
    try:
        write_frame_atomic(slices, slices_path, output_format)
        created.append(slices_path)
        write_frame_atomic(summary, summary_path, output_format)
        created.append(summary_path)
        write_text_atomic(report, report_path)
        created.append(report_path)
        manifest = {
            "sensitivity_run_id": run_id,
            "revision_sensitivity_contract_version": (
                REVISION_SENSITIVITY_CONTRACT_VERSION
            ),
            "source_revision_review_id": review["revision_review_id"],
            "source_revision_review_sha256": review[
                "revision_review_sha256"
            ],
            "source_revision_package_id": package["revision_package_id"],
            "source_revision_package_sha256": package[
                "revision_package_sha256"
            ],
            "revised_candidate_id": package["revised_candidate"][
                "candidate_id"
            ],
            "revised_candidate_sha256": package[
                "revised_candidate_sha256"
            ],
            "artifacts": [
                {
                    "role": "slices",
                    "path": slices_path.name,
                    "sha256": file_sha256(slices_path),
                    "row_count": len(slices),
                },
                {
                    "role": "summary",
                    "path": summary_path.name,
                    "sha256": file_sha256(summary_path),
                    "row_count": len(summary),
                },
                {
                    "role": "report",
                    "path": report_path.name,
                    "sha256": file_sha256(report_path),
                    "row_count": None,
                },
            ],
            **{
                field: False
                for field in REVISION_SENSITIVITY_SAFETY_FIELDS
            },
        }
        manifest["manifest_sha256"] = digest(manifest)
        write_text_atomic(
            json.dumps(canonical(manifest), indent=2, sort_keys=True) + "\n",
            manifest_path,
        )
        created.append(manifest_path)
    except Exception:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return slices_path, summary_path, report_path, manifest_path


__all__ = [
    "REVISION_SENSITIVITY_CONTRACT_VERSION",
    "REVISION_SENSITIVITY_SAFETY_FIELDS",
    "IntervalPolicyRevisionSensitivityError",
    "read_frame",
    "run_revision_sensitivity",
    "verify_revision_sensitivity",
    "write_revision_sensitivity",
]
