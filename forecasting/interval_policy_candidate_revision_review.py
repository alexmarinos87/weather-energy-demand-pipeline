from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting._interval_policy_candidate_revision_review_common import (
    ALLOWED_REVIEW_DECISIONS,
    NEXT_ACTIONS,
    REVIEW_CONTRACT_VERSION,
    REVIEW_EFFECTS,
    REVIEW_ID_PATTERN,
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyCandidateRevisionReviewError,
    canonical,
    digest,
    required_text,
    unique_texts,
    utc_timestamp,
)
from forecasting.interval_policy_candidate_revision import (
    verify_candidate_revision_package,
)


def create_candidate_revision_review(
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
    *,
    review_decision: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_changes: Iterable[Any] = (),
    reviewed_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable named review without executing the next action."""
    package_document = dict(package)
    decision_document = dict(decision)
    verify_candidate_revision_package(
        package_document, decision_document, sensitivity_summary
    )
    review_decision = required_text(review_decision, "review_decision")
    if review_decision not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyCandidateRevisionReviewError(
            "review_decision must be accept_for_sensitivity_review, "
            "reject_revision_package, or request_package_revision."
        )
    changes = unique_texts(
        requested_changes,
        "requested_changes",
        required=review_decision == "request_package_revision",
        minimum_length=10,
    )
    if review_decision != "request_package_revision" and changes:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{review_decision} cannot include requested changes."
        )
    reviewed_at = utc_timestamp(
        reviewed_at_utc or datetime.now(timezone.utc), "reviewed_at_utc"
    )
    package_time = utc_timestamp(
        package_document.get("prepared_at_utc"), "package prepared_at_utc"
    )
    if reviewed_at < package_time:
        raise IntervalPolicyCandidateRevisionReviewError(
            "reviewed_at_utc cannot precede the revision package."
        )

    core = {
        "revision_package_id": package_document["revision_package_id"],
        "revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "revision_package_contract_version": package_document[
            "revision_package_contract_version"
        ],
        "package_prepared_at_utc": package_time.isoformat(),
        "source_decision_id": package_document["source_decision_id"],
        "source_decision_sha256": package_document[
            "source_decision_sha256"
        ],
        "sensitivity_run_id": package_document["sensitivity_run_id"],
        "trend_run_id": package_document["trend_run_id"],
        "sensitivity_summary_sha256": package_document[
            "sensitivity_summary_sha256"
        ],
        "source_candidate_id": package_document["source_candidate"][
            "candidate_id"
        ],
        "source_candidate_version": package_document["source_candidate"][
            "candidate_version"
        ],
        "source_candidate_sha256": package_document[
            "source_candidate_sha256"
        ],
        "revised_candidate_id": package_document["revised_candidate"][
            "candidate_id"
        ],
        "revised_candidate_version": package_document[
            "revised_candidate"
        ]["candidate_version"],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
        "threshold_changes": package_document["threshold_changes"],
        "requested_change_responses": package_document[
            "requested_change_responses"
        ],
        "source_scenario_evidence": decision_document["scenario_evidence"],
        "review_decision": review_decision,
        "review_effect": REVIEW_EFFECTS[review_decision],
        "next_action": NEXT_ACTIONS[review_decision],
        "reviewer_name": required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": required_text(reviewer_role, "reviewer_role"),
        "review_ticket": required_text(review_ticket, "review_ticket"),
        "rationale": required_text(
            rationale, "rationale", minimum_length=30
        ),
        "requested_changes": changes,
        "reviewed_at_utc": reviewed_at.isoformat(),
        "compatibility_status_at_review": package_document[
            "compatibility_status"
        ],
    }
    review = {
        "revision_review_id": "irv-" + digest(core)[:24],
        "revision_review_revision": 1,
        **core,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": review_decision
        != "reject_revision_package",
        **{field: False for field in REVIEW_SAFETY_FIELDS},
        "revision_review_contract_version": REVIEW_CONTRACT_VERSION,
    }
    review["revision_review_sha256"] = digest(review)
    verify_candidate_revision_review(
        review, package_document, decision_document, sensitivity_summary
    )
    return review


def verify_candidate_revision_review(
    review: Mapping[str, Any],
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> None:
    """Verify review integrity and the full G29/G27/G26 evidence chain."""
    if not isinstance(review, Mapping):
        raise IntervalPolicyCandidateRevisionReviewError(
            "Candidate revision review must be a JSON object."
        )
    document = dict(review)
    package_document = dict(package)
    decision_document = dict(decision)
    verify_candidate_revision_package(
        package_document, decision_document, sensitivity_summary
    )
    if document.get("revision_review_contract_version") != (
        REVIEW_CONTRACT_VERSION
    ):
        raise IntervalPolicyCandidateRevisionReviewError(
            "Unsupported candidate-revision review contract."
        )
    review_id = required_text(
        document.get("revision_review_id"), "revision_review_id"
    )
    if not REVIEW_ID_PATTERN.fullmatch(review_id):
        raise IntervalPolicyCandidateRevisionReviewError(
            "revision_review_id is malformed."
        )
    if document.get("revision_review_revision") != 1:
        raise IntervalPolicyCandidateRevisionReviewError(
            "revision_review_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "revision_review_sha256"
        }
    )
    if document.get("revision_review_sha256") != expected_hash:
        raise IntervalPolicyCandidateRevisionReviewError(
            "Candidate revision review hash is invalid."
        )

    bindings = {
        "revision_package_id": package_document["revision_package_id"],
        "revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "revision_package_contract_version": package_document[
            "revision_package_contract_version"
        ],
        "source_decision_id": package_document["source_decision_id"],
        "source_decision_sha256": package_document[
            "source_decision_sha256"
        ],
        "sensitivity_run_id": package_document["sensitivity_run_id"],
        "trend_run_id": package_document["trend_run_id"],
        "sensitivity_summary_sha256": package_document[
            "sensitivity_summary_sha256"
        ],
        "source_candidate_id": package_document["source_candidate"][
            "candidate_id"
        ],
        "source_candidate_version": package_document["source_candidate"][
            "candidate_version"
        ],
        "source_candidate_sha256": package_document[
            "source_candidate_sha256"
        ],
        "revised_candidate_id": package_document["revised_candidate"][
            "candidate_id"
        ],
        "revised_candidate_version": package_document[
            "revised_candidate"
        ]["candidate_version"],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
        "compatibility_status_at_review": package_document[
            "compatibility_status"
        ],
    }
    for field, expected in bindings.items():
        if document.get(field) != expected:
            raise IntervalPolicyCandidateRevisionReviewError(
                f"Candidate revision review {field} is inconsistent."
            )
    if canonical(document.get("threshold_changes")) != canonical(
        package_document["threshold_changes"]
    ):
        raise IntervalPolicyCandidateRevisionReviewError(
            "Review threshold_changes do not match the package."
        )
    if canonical(document.get("requested_change_responses")) != canonical(
        package_document["requested_change_responses"]
    ):
        raise IntervalPolicyCandidateRevisionReviewError(
            "Review requested-change responses do not match the package."
        )
    if canonical(document.get("source_scenario_evidence")) != canonical(
        decision_document["scenario_evidence"]
    ):
        raise IntervalPolicyCandidateRevisionReviewError(
            "Review scenario evidence does not match the source decision."
        )
    review_decision = document.get("review_decision")
    if review_decision not in ALLOWED_REVIEW_DECISIONS:
        raise IntervalPolicyCandidateRevisionReviewError(
            "review_decision is invalid."
        )
    if document.get("review_effect") != REVIEW_EFFECTS[review_decision]:
        raise IntervalPolicyCandidateRevisionReviewError(
            "review_effect is inconsistent."
        )
    if document.get("next_action") != NEXT_ACTIONS[review_decision]:
        raise IntervalPolicyCandidateRevisionReviewError(
            "next_action is inconsistent."
        )
    changes = unique_texts(
        document.get("requested_changes", ()),
        "requested_changes",
        required=review_decision == "request_package_revision",
        minimum_length=10,
    )
    if review_decision != "request_package_revision" and changes:
        raise IntervalPolicyCandidateRevisionReviewError(
            "Review decision fields are inconsistent."
        )
    reviewed_at = utc_timestamp(
        document.get("reviewed_at_utc"), "reviewed_at_utc"
    )
    package_time = utc_timestamp(
        package_document["prepared_at_utc"], "package prepared_at_utc"
    )
    if reviewed_at < package_time:
        raise IntervalPolicyCandidateRevisionReviewError(
            "Review predates the candidate revision package."
        )
    required_text(document.get("reviewer_name"), "reviewer_name")
    required_text(document.get("reviewer_role"), "reviewer_role")
    required_text(document.get("review_ticket"), "review_ticket")
    required_text(document.get("rationale"), "rationale", minimum_length=30)
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyCandidateRevisionReviewError(
            "Named human review must be confirmed."
        )
    expected_follow_up = review_decision != "reject_revision_package"
    if document.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyCandidateRevisionReviewError(
            "follow_up_human_action_required is inconsistent."
        )
    for field in REVIEW_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyCandidateRevisionReviewError(
                f"Candidate revision review safety field {field} must be false."
            )


def render_candidate_revision_review(review: Mapping[str, Any]) -> str:
    document = dict(review)
    lines = [
        "# Interval-monitoring candidate revision review",
        "",
        f"- Review ID: `{document['revision_review_id']}`",
        f"- Revision package: `{document['revision_package_id']}`",
        f"- Decision: `{document['review_decision']}`",
        f"- Revised candidate: `{document['revised_candidate_id']}`",
        f"- Reviewer: {document['reviewer_name']} ({document['reviewer_role']})",
        f"- Review ticket: `{document['review_ticket']}`",
        f"- Reviewed at: `{document['reviewed_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
        "## Threshold changes reviewed",
        "",
        "| Field | Source | Revised |",
        "| --- | ---: | ---: |",
    ]
    for item in document["threshold_changes"]:
        lines.append(
            f"| {item['field']} | {item['source_value']} | "
            f"{item['revised_value']} |"
        )
    if document["requested_changes"]:
        lines.extend(["", "## Requested package changes", ""])
        lines.extend(f"- {item}" for item in document["requested_changes"])
    lines.extend(
        [
            "",
            "This review records a named human judgement only.",
            "Acceptance makes the package eligible for a separately requested "
            "sensitivity review; it does not execute that review or activate "
            "thresholds.",
            "No active-policy update, interval recalibration, model, schedule, "
            "promotion, alert, deployment, or publication action is performed.",
            "",
        ]
    )
    return "\n".join(lines)


def write_candidate_revision_review(
    output_directory: Path,
    review: Mapping[str, Any],
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> tuple[Path, Path]:
    verify_candidate_revision_review(
        review, package, decision, sensitivity_summary
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    review_id = review["revision_review_id"]
    json_path = output_directory / (
        f"interval_policy_candidate_revision_review_{review_id}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_candidate_revision_review_{review_id}.md"
    )
    temporary_paths = [
        json_path.with_name(f".{json_path.name}.tmp"),
        markdown_path.with_name(f".{markdown_path.name}.tmp"),
    ]
    for candidate in (json_path, markdown_path, *temporary_paths):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary_paths[0].write_text(
            json.dumps(canonical(review), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_paths[1].write_text(
            render_candidate_revision_review(review), encoding="utf-8"
        )
        temporary_paths[0].replace(json_path)
        temporary_paths[1].replace(markdown_path)
    finally:
        for temporary in temporary_paths:
            temporary.unlink(missing_ok=True)
    return json_path, markdown_path


def read_frame(path: Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicyCandidateRevisionReviewError(
        "Sensitivity summary input must be CSV or Parquet."
    )


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{label} must be readable JSON."
        ) from exc
    if not isinstance(value, dict):
        raise IntervalPolicyCandidateRevisionReviewError(
            f"{label} must be a JSON object."
        )
    return value


__all__ = [
    "ALLOWED_REVIEW_DECISIONS",
    "REVIEW_CONTRACT_VERSION",
    "REVIEW_SAFETY_FIELDS",
    "IntervalPolicyCandidateRevisionReviewError",
    "create_candidate_revision_review",
    "read_frame",
    "read_json",
    "render_candidate_revision_review",
    "verify_candidate_revision_review",
    "write_candidate_revision_review",
]
