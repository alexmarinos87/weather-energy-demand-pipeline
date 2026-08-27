from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting._interval_policy_revision_sensitivity_common import (
    REVISION_SENSITIVITY_CONTRACT_VERSION,
    REVISION_SENSITIVITY_SAFETY_FIELDS,
    canonical,
    digest,
    utc_timestamp,
)


DISPOSITION_CONTRACT_VERSION = "interval-policy-revision-disposition-v1"
DISPOSITION_ID_PATTERN = re.compile(r"^iprd-[0-9a-f]{24}$")
ALLOWED_DISPOSITIONS = {
    "retain_active_policy",
    "reject_revised_candidate",
    "request_another_revision",
    "suitable_for_separate_implementation_proposal",
}
DISPOSITION_EFFECTS = {
    "retain_active_policy": "active_policy_retained",
    "reject_revised_candidate": "revised_candidate_rejected",
    "request_another_revision": "new_candidate_revision_required",
    "suitable_for_separate_implementation_proposal": (
        "separate_implementation_proposal_review_required"
    ),
}
DISPOSITION_SAFETY_FIELDS = (
    "implementation_authorized",
    "implementation_applied",
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "source_manifest_mutated",
    "source_summary_mutated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)
REQUIRED_SUMMARY_COLUMNS = {
    "sensitivity_run_id",
    "sensitivity_run_timestamp_utc",
    "trend_run_id",
    "scenario",
    "candidate_id",
    "candidate_role",
    "candidate_version",
    "retained_monitor_status",
    "active_reference_status",
    "candidate_status",
    "status_changed_from_active",
    "sensitivity_classification",
    "slice_count",
    "changed_slice_count",
    "human_review_required",
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
}


class IntervalPolicyRevisionDispositionError(ValueError):
    """Raised when reviewed revision-sensitivity evidence is malformed or unsafe."""


def _required_text(value: Any, name: str) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        raise IntervalPolicyRevisionDispositionError(f"{name} must be non-empty.")
    return text


def _boolean(series: pd.Series, name: str) -> pd.Series:
    if series.dtype == bool:
        return series.astype(bool)
    parsed = series.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}
    )
    if parsed.isna().any():
        raise IntervalPolicyRevisionDispositionError(
            f"{name} must contain boolean values."
        )
    return parsed.astype(bool)


def _integer(series: pd.Series, name: str, *, minimum: int = 0) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if (
        values.isna().any()
        or (values < minimum).any()
        or not (values % 1 == 0).all()
    ):
        raise IntervalPolicyRevisionDispositionError(
            f"{name} must contain integers of at least {minimum}."
        )
    return values.astype(int)


def _unique_texts(
    values: Iterable[Any], name: str, *, required: bool
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyRevisionDispositionError(
            f"{name} must be a list of strings."
        )
    result = [_required_text(value, name) for value in values]
    if required and not result:
        raise IntervalPolicyRevisionDispositionError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyRevisionDispositionError(
            f"{name} must not contain duplicates."
        )
    if any(len(item) < 10 for item in result):
        raise IntervalPolicyRevisionDispositionError(
            f"Each {name} item must contain at least 10 characters."
        )
    return result


def prepare_revision_sensitivity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate one complete retained G31 summary."""
    missing = sorted(REQUIRED_SUMMARY_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyRevisionDispositionError(
            "Revision sensitivity summary is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    text_columns = (
        "sensitivity_run_id",
        "trend_run_id",
        "scenario",
        "candidate_id",
        "candidate_role",
        "candidate_version",
        "retained_monitor_status",
        "active_reference_status",
        "candidate_status",
        "sensitivity_classification",
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
    )
    for column in text_columns:
        prepared[column] = prepared[column].map(
            lambda value, name=column: _required_text(value, name)
        )
    prepared["sensitivity_run_timestamp_utc"] = prepared[
        "sensitivity_run_timestamp_utc"
    ].map(
        lambda value: utc_timestamp(
            value, "sensitivity_run_timestamp_utc"
        )
    )
    for column in (
        "status_changed_from_active",
        "human_review_required",
        *REVISION_SENSITIVITY_SAFETY_FIELDS,
    ):
        prepared[column] = _boolean(prepared[column], column)
    prepared["slice_count"] = _integer(
        prepared["slice_count"], "slice_count", minimum=1
    )
    prepared["changed_slice_count"] = _integer(
        prepared["changed_slice_count"],
        "changed_slice_count",
        minimum=0,
    )
    if (
        prepared["changed_slice_count"] > prepared["slice_count"]
    ).any():
        raise IntervalPolicyRevisionDispositionError(
            "changed_slice_count cannot exceed slice_count."
        )
    if prepared["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyRevisionDispositionError(
            "Exactly one revision sensitivity run is required."
        )
    if prepared["sensitivity_run_timestamp_utc"].nunique() != 1:
        raise IntervalPolicyRevisionDispositionError(
            "Exactly one revision sensitivity timestamp is required."
        )
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicyRevisionDispositionError(
            "Exactly one retained trend run is required."
        )
    if set(prepared["revision_sensitivity_contract_version"]) != {
        REVISION_SENSITIVITY_CONTRACT_VERSION
    }:
        raise IntervalPolicyRevisionDispositionError(
            "interval-policy-revision-sensitivity-v1 evidence is required."
        )
    if prepared[list(REVISION_SENSITIVITY_SAFETY_FIELDS)].any(axis=None):
        raise IntervalPolicyRevisionDispositionError(
            "Revision sensitivity evidence contains enabled authority fields."
        )
    for column in (
        "source_revision_review_id",
        "source_revision_review_sha256",
        "source_revision_package_id",
        "source_revision_package_sha256",
        "source_decision_id",
        "source_decision_sha256",
        "revised_candidate_id",
        "revised_candidate_version",
        "revised_candidate_sha256",
    ):
        if prepared[column].nunique() != 1:
            raise IntervalPolicyRevisionDispositionError(
                f"{column} must be constant within the run."
            )
    revised_id = prepared["revised_candidate_id"].iloc[0]
    if set(prepared["candidate_id"]) != {"active-reference", revised_id}:
        raise IntervalPolicyRevisionDispositionError(
            "Summary must contain exactly active-reference and the revised candidate."
        )
    if set(prepared["candidate_role"]) != {
        "active_reference",
        "review_candidate",
    }:
        raise IntervalPolicyRevisionDispositionError(
            "Candidate roles are inconsistent."
        )
    identity = ["scenario", "candidate_id"]
    if prepared.duplicated(subset=identity, keep=False).any():
        raise IntervalPolicyRevisionDispositionError(
            "Duplicate scenario/candidate identities are not allowed."
        )
    scenario_sets = {
        candidate_id: set(group["scenario"])
        for candidate_id, group in prepared.groupby(
            "candidate_id", sort=False
        )
    }
    if len({frozenset(value) for value in scenario_sets.values()}) != 1:
        raise IntervalPolicyRevisionDispositionError(
            "Both candidates must cover the same scenarios."
        )
    active = prepared.loc[
        prepared["candidate_id"] == "active-reference"
    ]
    if not (
        (active["candidate_status"] == active["retained_monitor_status"])
        & (
            active["active_reference_status"]
            == active["retained_monitor_status"]
        )
        & (~active["status_changed_from_active"])
        & (active["changed_slice_count"] == 0)
    ).all():
        raise IntervalPolicyRevisionDispositionError(
            "Active-reference evidence does not reproduce retained monitoring."
        )
    revised = prepared.loc[prepared["candidate_id"] == revised_id]
    expected_changed = (
        revised["candidate_status"] != revised["active_reference_status"]
    )
    if not (
        revised["status_changed_from_active"] == expected_changed
    ).all():
        raise IntervalPolicyRevisionDispositionError(
            "Revised-candidate status-change evidence is inconsistent."
        )
    return prepared.sort_values(identity).reset_index(drop=True)


def revision_sensitivity_summary_sha256(frame: pd.DataFrame) -> str:
    prepared = prepare_revision_sensitivity_summary(frame)
    return digest(prepared.to_dict(orient="records"))


def verify_revision_sensitivity_manifest(
    manifest: Mapping[str, Any],
    summary: pd.DataFrame,
) -> None:
    """Verify the G31 manifest binding needed by a disposition."""
    document = dict(manifest)
    prepared = prepare_revision_sensitivity_summary(summary)
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "manifest_sha256"
        }
    )
    if document.get("manifest_sha256") != expected_hash:
        raise IntervalPolicyRevisionDispositionError(
            "Revision sensitivity manifest hash is invalid."
        )
    if document.get("revision_sensitivity_contract_version") != (
        REVISION_SENSITIVITY_CONTRACT_VERSION
    ):
        raise IntervalPolicyRevisionDispositionError(
            "Revision sensitivity manifest contract is invalid."
        )
    if document.get("sensitivity_run_id") != prepared[
        "sensitivity_run_id"
    ].iloc[0]:
        raise IntervalPolicyRevisionDispositionError(
            "Manifest and summary sensitivity_run_id values differ."
        )
    for field in (
        "source_revision_review_id",
        "source_revision_review_sha256",
        "source_revision_package_id",
        "source_revision_package_sha256",
        "revised_candidate_id",
        "revised_candidate_sha256",
    ):
        if document.get(field) != prepared[field].iloc[0]:
            raise IntervalPolicyRevisionDispositionError(
                f"Manifest {field} does not match the summary."
            )
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or {
        item.get("role") for item in artifacts if isinstance(item, dict)
    } != {"slices", "summary", "report"}:
        raise IntervalPolicyRevisionDispositionError(
            "Manifest must bind slices, summary, and report artifacts."
        )
    for field in REVISION_SENSITIVITY_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyRevisionDispositionError(
                f"Manifest safety field {field} must be false."
            )


def create_revision_sensitivity_disposition(
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    disposition: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_actions: Iterable[Any] = (),
    disposed_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable named human disposition over G31 evidence."""
    prepared = prepare_revision_sensitivity_summary(summary)
    verify_revision_sensitivity_manifest(manifest, prepared)
    disposition = _required_text(disposition, "disposition")
    if disposition not in ALLOWED_DISPOSITIONS:
        raise IntervalPolicyRevisionDispositionError(
            "Unsupported revision sensitivity disposition."
        )
    revised_id = prepared["revised_candidate_id"].iloc[0]
    target_id = (
        "active-reference"
        if disposition == "retain_active_policy"
        else revised_id
    )
    requested = _unique_texts(
        requested_actions,
        "requested_actions",
        required=disposition == "request_another_revision",
    )
    if disposition != "request_another_revision" and requested:
        raise IntervalPolicyRevisionDispositionError(
            "Only request_another_revision may contain requested_actions."
        )
    disposed_at = utc_timestamp(
        disposed_at_utc or datetime.now(timezone.utc),
        "disposed_at_utc",
    )
    source_timestamp = prepared[
        "sensitivity_run_timestamp_utc"
    ].iloc[0]
    if disposed_at < source_timestamp:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition cannot precede revision sensitivity evidence."
        )
    target = prepared.loc[prepared["candidate_id"] == target_id]
    scenario_evidence = [
        {
            "scenario": row.scenario,
            "retained_monitor_status": row.retained_monitor_status,
            "active_reference_status": row.active_reference_status,
            "target_candidate_status": row.candidate_status,
            "status_changed_from_active": bool(
                row.status_changed_from_active
            ),
            "sensitivity_classification": row.sensitivity_classification,
            "changed_slice_count": int(row.changed_slice_count),
            "human_review_required": bool(row.human_review_required),
        }
        for row in target.sort_values("scenario").itertuples(index=False)
    ]
    core = {
        "sensitivity_run_id": prepared["sensitivity_run_id"].iloc[0],
        "sensitivity_run_timestamp_utc": source_timestamp.isoformat(),
        "trend_run_id": prepared["trend_run_id"].iloc[0],
        "revision_sensitivity_summary_sha256": (
            revision_sensitivity_summary_sha256(prepared)
        ),
        "revision_sensitivity_manifest_sha256": manifest[
            "manifest_sha256"
        ],
        "source_revision_review_id": prepared[
            "source_revision_review_id"
        ].iloc[0],
        "source_revision_package_id": prepared[
            "source_revision_package_id"
        ].iloc[0],
        "source_decision_id": prepared["source_decision_id"].iloc[0],
        "revised_candidate_id": revised_id,
        "revised_candidate_version": prepared[
            "revised_candidate_version"
        ].iloc[0],
        "revised_candidate_sha256": prepared[
            "revised_candidate_sha256"
        ].iloc[0],
        "disposition": disposition,
        "disposition_effect": DISPOSITION_EFFECTS[disposition],
        "target_candidate_id": target_id,
        "reviewer_name": _required_text(reviewer_name, "reviewer_name"),
        "reviewer_role": _required_text(reviewer_role, "reviewer_role"),
        "review_ticket": _required_text(review_ticket, "review_ticket"),
        "rationale": _required_text(rationale, "rationale"),
        "requested_actions": requested,
        "disposed_at_utc": disposed_at.isoformat(),
        "scenario_evidence": scenario_evidence,
    }
    document = {
        "disposition_id": "iprd-" + digest(core)[:24],
        "disposition_revision": 1,
        **core,
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": disposition in {
            "request_another_revision",
            "suitable_for_separate_implementation_proposal",
        },
        **{field: False for field in DISPOSITION_SAFETY_FIELDS},
        "disposition_contract_version": DISPOSITION_CONTRACT_VERSION,
    }
    document["disposition_sha256"] = digest(document)
    verify_revision_sensitivity_disposition(
        document, prepared, manifest
    )
    return document


def verify_revision_sensitivity_disposition(
    disposition: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
) -> None:
    """Verify a G32 disposition and all retained bindings."""
    document = dict(disposition)
    prepared = prepare_revision_sensitivity_summary(summary)
    verify_revision_sensitivity_manifest(manifest, prepared)
    if document.get("disposition_contract_version") != (
        DISPOSITION_CONTRACT_VERSION
    ):
        raise IntervalPolicyRevisionDispositionError(
            "Disposition contract version is invalid."
        )
    disposition_id = _required_text(
        document.get("disposition_id"), "disposition_id"
    )
    if not DISPOSITION_ID_PATTERN.fullmatch(disposition_id):
        raise IntervalPolicyRevisionDispositionError(
            "disposition_id is malformed."
        )
    if document.get("disposition_revision") != 1:
        raise IntervalPolicyRevisionDispositionError(
            "disposition_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "disposition_sha256"
        }
    )
    if document.get("disposition_sha256") != expected_hash:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition hash is invalid."
        )
    if document.get("revision_sensitivity_summary_sha256") != (
        revision_sensitivity_summary_sha256(prepared)
    ):
        raise IntervalPolicyRevisionDispositionError(
            "Disposition does not match the retained summary."
        )
    if document.get("revision_sensitivity_manifest_sha256") != (
        manifest["manifest_sha256"]
    ):
        raise IntervalPolicyRevisionDispositionError(
            "Disposition does not match the retained manifest."
        )
    disposition_name = document.get("disposition")
    if disposition_name not in ALLOWED_DISPOSITIONS:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition decision is invalid."
        )
    if document.get("disposition_effect") != DISPOSITION_EFFECTS[
        disposition_name
    ]:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition effect is inconsistent."
        )
    revised_id = prepared["revised_candidate_id"].iloc[0]
    expected_target = (
        "active-reference"
        if disposition_name == "retain_active_policy"
        else revised_id
    )
    if document.get("target_candidate_id") != expected_target:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition target is inconsistent."
        )
    requested = _unique_texts(
        document.get("requested_actions", ()),
        "requested_actions",
        required=disposition_name == "request_another_revision",
    )
    if disposition_name != "request_another_revision" and requested:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition requested actions are inconsistent."
        )
    for field in (
        "sensitivity_run_id",
        "trend_run_id",
        "source_revision_review_id",
        "source_revision_package_id",
        "source_decision_id",
        "revised_candidate_id",
        "revised_candidate_version",
        "revised_candidate_sha256",
    ):
        if document.get(field) != prepared[field].iloc[0]:
            raise IntervalPolicyRevisionDispositionError(
                f"Disposition {field} is inconsistent."
            )
    _required_text(document.get("reviewer_name"), "reviewer_name")
    _required_text(document.get("reviewer_role"), "reviewer_role")
    _required_text(document.get("review_ticket"), "review_ticket")
    _required_text(document.get("rationale"), "rationale")
    disposed_at = utc_timestamp(
        document.get("disposed_at_utc"), "disposed_at_utc"
    )
    if disposed_at < prepared[
        "sensitivity_run_timestamp_utc"
    ].iloc[0]:
        raise IntervalPolicyRevisionDispositionError(
            "Disposition timestamp precedes the source evidence."
        )
    target = prepared.loc[
        prepared["candidate_id"] == expected_target
    ]
    expected_scenarios = [
        {
            "scenario": row.scenario,
            "retained_monitor_status": row.retained_monitor_status,
            "active_reference_status": row.active_reference_status,
            "target_candidate_status": row.candidate_status,
            "status_changed_from_active": bool(
                row.status_changed_from_active
            ),
            "sensitivity_classification": row.sensitivity_classification,
            "changed_slice_count": int(row.changed_slice_count),
            "human_review_required": bool(row.human_review_required),
        }
        for row in target.sort_values("scenario").itertuples(index=False)
    ]
    if canonical(document.get("scenario_evidence")) != canonical(
        expected_scenarios
    ):
        raise IntervalPolicyRevisionDispositionError(
            "Disposition scenario evidence is inconsistent."
        )
    if document.get("named_human_review_confirmed") is not True:
        raise IntervalPolicyRevisionDispositionError(
            "Named human review must be confirmed."
        )
    expected_follow_up = disposition_name in {
        "request_another_revision",
        "suitable_for_separate_implementation_proposal",
    }
    if document.get("follow_up_human_action_required") is not expected_follow_up:
        raise IntervalPolicyRevisionDispositionError(
            "Follow-up human-action evidence is inconsistent."
        )
    for field in DISPOSITION_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyRevisionDispositionError(
                f"Disposition safety field {field} must be false."
            )


def render_revision_sensitivity_disposition(
    disposition: Mapping[str, Any],
) -> str:
    document = dict(disposition)
    lines = [
        "# Interval-policy revision sensitivity disposition",
        "",
        f"- Disposition ID: `{document['disposition_id']}`",
        f"- Sensitivity run: `{document['sensitivity_run_id']}`",
        f"- Decision: `{document['disposition']}`",
        f"- Target: `{document['target_candidate_id']}`",
        f"- Reviewer: {document['reviewer_name']} ({document['reviewer_role']})",
        f"- Review ticket: `{document['review_ticket']}`",
        f"- Disposed at: `{document['disposed_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
    ]
    if document["requested_actions"]:
        lines.extend(["## Requested actions", ""])
        lines.extend(
            f"- {item}" for item in document["requested_actions"]
        )
        lines.append("")
    lines.extend(
        [
            "This disposition is immutable human review evidence only.",
            "It does not update or activate the monitoring policy, recalibrate "
            "intervals, change models or schedules, deliver alerts, deploy, "
            "or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def read_frame(path: Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicyRevisionDispositionError(
        "Revision sensitivity summary must be CSV or Parquet."
    )


def write_revision_sensitivity_disposition(
    output_directory: Path,
    disposition: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
) -> tuple[Path, Path]:
    document = dict(disposition)
    verify_revision_sensitivity_disposition(
        document, summary, manifest
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / (
        f"interval_policy_revision_disposition_"
        f"{document['disposition_id']}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_revision_disposition_"
        f"{document['disposition_id']}.md"
    )
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(
        f".{markdown_path.name}.tmp"
    )
    for path in (
        json_path,
        markdown_path,
        temporary_json,
        temporary_markdown,
    ):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary_json.write_text(
            json.dumps(canonical(document), indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(
            render_revision_sensitivity_disposition(document),
            encoding="utf-8",
        )
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return json_path, markdown_path


__all__ = [
    "ALLOWED_DISPOSITIONS",
    "DISPOSITION_CONTRACT_VERSION",
    "DISPOSITION_SAFETY_FIELDS",
    "IntervalPolicyRevisionDispositionError",
    "create_revision_sensitivity_disposition",
    "prepare_revision_sensitivity_summary",
    "read_frame",
    "render_revision_sensitivity_disposition",
    "revision_sensitivity_summary_sha256",
    "verify_revision_sensitivity_disposition",
    "verify_revision_sensitivity_manifest",
    "write_revision_sensitivity_disposition",
]
