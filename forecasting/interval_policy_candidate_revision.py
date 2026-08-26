from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import (
    CONFIG_FIELDS,
    PACKAGE_SAFETY_FIELDS,
    REVISION_PACKAGE_CONTRACT_VERSION,
    REVISION_PACKAGE_ID_PATTERN,
    IntervalPolicyCandidateRevisionError,
    active_policy_snapshot,
    canonical,
    digest,
    prepare_candidate_snapshot,
    prepare_change_responses,
    required_text,
    threshold_changes,
    utc_timestamp,
)
from forecasting.interval_policy_review_decision import (
    verify_policy_review_decision,
)


def create_candidate_revision_package(
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
    *,
    source_candidate: Mapping[str, Any],
    revised_candidate: Mapping[str, Any],
    requested_change_responses: Iterable[Mapping[str, Any]],
    prepared_by: str,
    preparer_role: str,
    revision_ticket: str,
    rationale: str,
    prepared_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable, non-activating candidate-revision package."""
    decision_document = dict(decision)
    verify_policy_review_decision(decision_document, sensitivity_summary)
    if decision_document.get("decision") != "request_candidate_revision":
        raise IntervalPolicyCandidateRevisionError(
            "A candidate revision package requires request_candidate_revision evidence."
        )
    if decision_document.get("follow_up_human_action_required") is not True:
        raise IntervalPolicyCandidateRevisionError(
            "The source decision must require human follow-up."
        )

    source = prepare_candidate_snapshot(source_candidate, label="source_candidate")
    revised = prepare_candidate_snapshot(revised_candidate, label="revised_candidate")
    if source["candidate_id"] != decision_document.get("target_candidate_id"):
        raise IntervalPolicyCandidateRevisionError(
            "source_candidate does not match the decision target candidate."
        )
    if source["candidate_version"] != decision_document.get(
        "target_candidate_version"
    ):
        raise IntervalPolicyCandidateRevisionError(
            "source_candidate version does not match the decision target."
        )
    if revised["candidate_id"] == source["candidate_id"]:
        raise IntervalPolicyCandidateRevisionError(
            "revised_candidate must use a new candidate_id."
        )
    if revised["candidate_version"] == source["candidate_version"]:
        raise IntervalPolicyCandidateRevisionError(
            "revised_candidate must use a new candidate_version."
        )

    changes = threshold_changes(source, revised)
    if not changes:
        raise IntervalPolicyCandidateRevisionError(
            "A revision package must change at least one monitoring threshold."
        )
    responses = prepare_change_responses(
        requested_change_responses,
        requested_changes=decision_document.get("requested_changes", ()),
        changed_fields=[item["field"] for item in changes],
    )
    prepared_at = utc_timestamp(
        prepared_at_utc or datetime.now(timezone.utc), "prepared_at_utc"
    )
    decision_time = utc_timestamp(
        decision_document.get("decision_timestamp_utc"),
        "decision_timestamp_utc",
    )
    if prepared_at < decision_time:
        raise IntervalPolicyCandidateRevisionError(
            "prepared_at_utc cannot precede the source decision."
        )
    active = active_policy_snapshot()
    core = {
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "source_decision_timestamp_utc": decision_time.isoformat(),
        "sensitivity_run_id": decision_document["sensitivity_run_id"],
        "trend_run_id": decision_document["trend_run_id"],
        "sensitivity_summary_sha256": decision_document[
            "sensitivity_summary_sha256"
        ],
        "requested_changes": list(decision_document["requested_changes"]),
        "source_candidate": source,
        "source_candidate_sha256": digest(source),
        "revised_candidate": revised,
        "revised_candidate_sha256": digest(revised),
        "active_policy_snapshot": active,
        "active_policy_sha256": digest(active),
        "threshold_changes": changes,
        "requested_change_responses": responses,
        "prepared_by": required_text(prepared_by, "prepared_by"),
        "preparer_role": required_text(preparer_role, "preparer_role"),
        "revision_ticket": required_text(revision_ticket, "revision_ticket"),
        "rationale": required_text(rationale, "rationale", minimum_length=30),
        "prepared_at_utc": prepared_at.isoformat(),
        "compatibility_status": "compatible_for_new_sensitivity_review",
        "next_review_action": "named_revision_package_review_required",
    }
    package = {
        "revision_package_id": "ipr-" + digest(core)[:24],
        "revision_package_revision": 1,
        **core,
        "human_authored_revision_confirmed": True,
        "automatic_sensitivity_rerun_allowed": False,
        **{field: False for field in PACKAGE_SAFETY_FIELDS},
        "revision_package_contract_version": REVISION_PACKAGE_CONTRACT_VERSION,
    }
    package["revision_package_sha256"] = digest(package)
    verify_candidate_revision_package(
        package, decision_document, sensitivity_summary
    )
    return package


def verify_candidate_revision_package(
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> None:
    """Verify package integrity and the complete G27/G26 evidence binding."""
    if not isinstance(package, Mapping):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package must be a JSON object."
        )
    document = dict(package)
    decision_document = dict(decision)
    verify_policy_review_decision(decision_document, sensitivity_summary)
    if decision_document.get("decision") != "request_candidate_revision":
        raise IntervalPolicyCandidateRevisionError(
            "The source decision is not a revision request."
        )
    if document.get("revision_package_contract_version") != (
        REVISION_PACKAGE_CONTRACT_VERSION
    ):
        raise IntervalPolicyCandidateRevisionError(
            "Unsupported candidate-revision package contract."
        )
    package_id = required_text(
        document.get("revision_package_id"), "revision_package_id"
    )
    if not REVISION_PACKAGE_ID_PATTERN.fullmatch(package_id):
        raise IntervalPolicyCandidateRevisionError(
            "revision_package_id is malformed."
        )
    if document.get("revision_package_revision") != 1:
        raise IntervalPolicyCandidateRevisionError(
            "revision_package_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "revision_package_sha256"
        }
    )
    if document.get("revision_package_sha256") != expected_hash:
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package hash is invalid."
        )
    bindings = {
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "sensitivity_run_id": decision_document["sensitivity_run_id"],
        "trend_run_id": decision_document["trend_run_id"],
        "sensitivity_summary_sha256": decision_document[
            "sensitivity_summary_sha256"
        ],
    }
    for field, expected in bindings.items():
        if document.get(field) != expected:
            raise IntervalPolicyCandidateRevisionError(
                f"Candidate revision package {field} is inconsistent."
            )
    source = prepare_candidate_snapshot(
        document.get("source_candidate", {}), label="source_candidate"
    )
    revised = prepare_candidate_snapshot(
        document.get("revised_candidate", {}), label="revised_candidate"
    )
    if source["candidate_id"] != decision_document["target_candidate_id"]:
        raise IntervalPolicyCandidateRevisionError(
            "Package source candidate does not match the decision target."
        )
    if source["candidate_version"] != decision_document[
        "target_candidate_version"
    ]:
        raise IntervalPolicyCandidateRevisionError(
            "Package source candidate version is inconsistent."
        )
    if document.get("source_candidate_sha256") != digest(source):
        raise IntervalPolicyCandidateRevisionError(
            "source_candidate_sha256 is invalid."
        )
    if document.get("revised_candidate_sha256") != digest(revised):
        raise IntervalPolicyCandidateRevisionError(
            "revised_candidate_sha256 is invalid."
        )
    if revised["candidate_id"] == source["candidate_id"]:
        raise IntervalPolicyCandidateRevisionError(
            "Revised candidate ID must differ from the source candidate."
        )
    if revised["candidate_version"] == source["candidate_version"]:
        raise IntervalPolicyCandidateRevisionError(
            "Revised candidate version must differ from the source candidate."
        )
    expected_changes = threshold_changes(source, revised)
    if not expected_changes or canonical(document.get("threshold_changes")) != (
        canonical(expected_changes)
    ):
        raise IntervalPolicyCandidateRevisionError(
            "threshold_changes are incomplete or inconsistent."
        )
    expected_responses = prepare_change_responses(
        document.get("requested_change_responses", ()),
        requested_changes=decision_document["requested_changes"],
        changed_fields=[item["field"] for item in expected_changes],
    )
    if canonical(document.get("requested_change_responses")) != canonical(
        expected_responses
    ):
        raise IntervalPolicyCandidateRevisionError(
            "requested_change_responses are inconsistent."
        )
    active = active_policy_snapshot()
    if canonical(document.get("active_policy_snapshot")) != canonical(active):
        raise IntervalPolicyCandidateRevisionError(
            "active_policy_snapshot does not match the checked-in policy."
        )
    if document.get("active_policy_sha256") != digest(active):
        raise IntervalPolicyCandidateRevisionError(
            "active_policy_sha256 is invalid."
        )
    prepared_at = utc_timestamp(
        document.get("prepared_at_utc"), "prepared_at_utc"
    )
    decision_at = utc_timestamp(
        decision_document["decision_timestamp_utc"],
        "decision_timestamp_utc",
    )
    if prepared_at < decision_at:
        raise IntervalPolicyCandidateRevisionError(
            "Package predates the source decision."
        )
    required_text(document.get("prepared_by"), "prepared_by")
    required_text(document.get("preparer_role"), "preparer_role")
    required_text(document.get("revision_ticket"), "revision_ticket")
    required_text(document.get("rationale"), "rationale", minimum_length=30)
    if document.get("requested_changes") != decision_document[
        "requested_changes"
    ]:
        raise IntervalPolicyCandidateRevisionError(
            "Package requested_changes do not match the source decision."
        )
    if document.get("compatibility_status") != (
        "compatible_for_new_sensitivity_review"
    ):
        raise IntervalPolicyCandidateRevisionError(
            "compatibility_status is invalid."
        )
    if document.get("next_review_action") != (
        "named_revision_package_review_required"
    ):
        raise IntervalPolicyCandidateRevisionError(
            "next_review_action is invalid."
        )
    if document.get("human_authored_revision_confirmed") is not True:
        raise IntervalPolicyCandidateRevisionError(
            "Human-authored revision must be confirmed."
        )
    if document.get("automatic_sensitivity_rerun_allowed") is not False:
        raise IntervalPolicyCandidateRevisionError(
            "Automatic sensitivity rerun must remain disabled."
        )
    for field in PACKAGE_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyCandidateRevisionError(
                f"Revision package safety field {field} must be false."
            )


def render_candidate_revision_package(package: Mapping[str, Any]) -> str:
    document = dict(package)
    lines = [
        "# Interval-monitoring candidate revision package",
        "",
        f"- Package ID: `{document['revision_package_id']}`",
        f"- Source decision: `{document['source_decision_id']}`",
        f"- Source candidate: `{document['source_candidate']['candidate_id']}`",
        f"- Revised candidate: `{document['revised_candidate']['candidate_id']}`",
        f"- Prepared by: {document['prepared_by']} ({document['preparer_role']})",
        f"- Revision ticket: `{document['revision_ticket']}`",
        f"- Prepared at: `{document['prepared_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
        "## Threshold changes",
        "",
        "| Field | Source | Revised |",
        "| --- | ---: | ---: |",
    ]
    for item in document["threshold_changes"]:
        lines.append(
            f"| {item['field']} | {item['source_value']} | {item['revised_value']} |"
        )
    lines.extend(["", "## Requested-change responses", ""])
    for item in document["requested_change_responses"]:
        lines.extend(
            [
                f"- **Request:** {item['requested_change']}",
                f"  - Response: {item['response']}",
                "  - Thresholds: "
                + ", ".join(item["changed_threshold_fields"]),
            ]
        )
    lines.extend(
        [
            "",
            "This package is compatible only for a new, separately reviewed sensitivity comparison.",
            "It does not update or activate the checked-in monitoring policy and performs no interval recalibration, model, schedule, promotion, alert, deployment, or publication action.",
            "",
        ]
    )
    return "\n".join(lines)


def write_candidate_revision_package(
    output_directory: Path,
    package: Mapping[str, Any],
    decision: Mapping[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> tuple[Path, Path]:
    verify_candidate_revision_package(package, decision, sensitivity_summary)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    package_id = package["revision_package_id"]
    json_path = output_directory / (
        f"interval_policy_candidate_revision_{package_id}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_candidate_revision_{package_id}.md"
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
            json.dumps(canonical(package), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_paths[1].write_text(
            render_candidate_revision_package(package), encoding="utf-8"
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
    raise IntervalPolicyCandidateRevisionError(
        "Sensitivity summary input must be CSV or Parquet."
    )


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"{label} must be readable JSON."
        ) from exc
    if not isinstance(value, dict):
        raise IntervalPolicyCandidateRevisionError(
            f"{label} must be a JSON object."
        )
    return value


__all__ = [
    "CONFIG_FIELDS",
    "PACKAGE_SAFETY_FIELDS",
    "REVISION_PACKAGE_CONTRACT_VERSION",
    "IntervalPolicyCandidateRevisionError",
    "create_candidate_revision_package",
    "read_frame",
    "read_json",
    "render_candidate_revision_package",
    "verify_candidate_revision_package",
    "write_candidate_revision_package",
]
