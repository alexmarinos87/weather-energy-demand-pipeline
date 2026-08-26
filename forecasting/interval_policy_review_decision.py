from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DECISION_CONTRACT_VERSION = "interval-policy-review-decision-v1"
SENSITIVITY_CONTRACT_VERSION = "interval-policy-sensitivity-v1"
DECISION_ID_PATTERN = re.compile(r"^ipd-[0-9a-f]{24}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_DECISIONS = {
    "retain_active_policy",
    "reject_candidate",
    "request_revision",
}
DECISION_EFFECTS = {
    "retain_active_policy": "active_policy_retained_for_review",
    "reject_candidate": "review_candidate_rejected",
    "request_revision": "review_candidate_revision_requested",
}
SUPPORTED_STATUSES = {"healthy", "warning", "failed"}
SUPPORTED_CLASSIFICATIONS = {
    "active_reference",
    "status_robust",
    "status_sensitive",
}
AUTHORITY_FIELDS = (
    "active_policy_updated",
    "candidate_thresholds_activated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
)


class IntervalPolicyReviewDecisionError(ValueError):
    """Raised when a named monitoring-policy decision is unsafe or inconsistent."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def _bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().casefold()
    if text == "true":
        return True
    if text == "false":
        return False
    raise IntervalPolicyReviewDecisionError(f"{name} must be boolean.")


def _column(frame: pd.DataFrame, canonical: str, aliases: Iterable[str] = ()) -> str:
    for candidate in (canonical, *aliases):
        if candidate in frame.columns:
            return candidate
    names = ", ".join((canonical, *aliases))
    raise IntervalPolicyReviewDecisionError(
        f"Sensitivity summary is missing required column {canonical}; accepted names: {names}."
    )


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return _utc(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _digest(document: Any) -> str:
    encoded = json.dumps(
        _canonical(document), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prepare_sensitivity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize one complete retained G26 sensitivity summary run."""
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary must contain retained evidence rows."
        )
    mapping = {
        "sensitivity_run_id": _column(frame, "sensitivity_run_id"),
        "sensitivity_run_timestamp_utc": _column(frame, "sensitivity_run_timestamp_utc"),
        "trend_run_id": _column(frame, "trend_run_id"),
        "scenario": _column(frame, "scenario"),
        "policy_id": _column(frame, "candidate_policy_id", ("policy_id",)),
        "policy_role": _column(frame, "candidate_policy_role", ("policy_role",)),
        "policy_version": _column(frame, "candidate_policy_version", ("policy_version",)),
        "retained_monitor_status": _column(frame, "retained_monitor_status"),
        "active_reference_status": _column(frame, "active_reference_status"),
        "candidate_status": _column(frame, "candidate_status"),
        "sensitivity_classification": _column(frame, "sensitivity_classification"),
        "status_changed_from_active": _column(frame, "status_changed_from_active"),
        "changed_slice_count": _column(frame, "changed_slice_count"),
        "human_review_required": _column(frame, "human_review_required"),
        "sensitivity_contract_version": _column(frame, "sensitivity_contract_version"),
    }
    prepared = pd.DataFrame({name: frame[source] for name, source in mapping.items()})
    for name in (
        "sensitivity_run_id", "trend_run_id", "scenario", "policy_id",
        "policy_role", "policy_version", "retained_monitor_status",
        "active_reference_status", "candidate_status",
        "sensitivity_classification", "sensitivity_contract_version",
    ):
        prepared[name] = prepared[name].map(lambda value, n=name: _required_text(value, n))
    prepared["sensitivity_run_timestamp_utc"] = prepared[
        "sensitivity_run_timestamp_utc"
    ].map(lambda value: _utc(value, "sensitivity_run_timestamp_utc"))
    prepared["status_changed_from_active"] = prepared[
        "status_changed_from_active"
    ].map(lambda value: _bool(value, "status_changed_from_active"))
    prepared["human_review_required"] = prepared["human_review_required"].map(
        lambda value: _bool(value, "human_review_required")
    )
    counts = pd.to_numeric(prepared["changed_slice_count"], errors="coerce")
    if counts.isna().any() or (counts < 0).any() or not (counts % 1 == 0).all():
        raise IntervalPolicyReviewDecisionError(
            "changed_slice_count must contain non-negative integers."
        )
    prepared["changed_slice_count"] = counts.astype(int)

    for field in AUTHORITY_FIELDS:
        source = _column(frame, field)
        values = frame[source].map(lambda value, name=field: _bool(value, name))
        if values.any():
            raise IntervalPolicyReviewDecisionError(
                f"Sensitivity authority field {field} must remain false."
            )
        prepared[field] = False

    if prepared["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "Decision evidence must bind exactly one sensitivity_run_id."
        )
    if prepared["sensitivity_run_timestamp_utc"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "One sensitivity run must have exactly one timestamp."
        )
    if prepared["trend_run_id"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "One sensitivity run must bind exactly one trend_run_id."
        )
    if set(prepared["sensitivity_contract_version"]) != {
        SENSITIVITY_CONTRACT_VERSION
    }:
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary uses an unsupported contract version."
        )
    if not set(prepared["retained_monitor_status"]).issubset(SUPPORTED_STATUSES):
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary contains an unsupported retained status."
        )
    for column_name in ("active_reference_status", "candidate_status"):
        if not set(prepared[column_name]).issubset(SUPPORTED_STATUSES):
            raise IntervalPolicyReviewDecisionError(
                f"Sensitivity summary contains an unsupported {column_name}."
            )
    if not set(prepared["sensitivity_classification"]).issubset(
        SUPPORTED_CLASSIFICATIONS
    ):
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary contains an unsupported sensitivity classification."
        )
    if prepared.duplicated(subset=["scenario", "policy_id"], keep=False).any():
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary contains duplicate scenario/policy identities."
        )
    policy_identity = prepared.groupby("policy_id", sort=False).agg(
        role_count=("policy_role", "nunique"),
        version_count=("policy_version", "nunique"),
    )
    if (policy_identity != 1).any(axis=None):
        raise IntervalPolicyReviewDecisionError(
            "Each sensitivity policy must have one role and version."
        )
    active_ids = prepared.loc[
        prepared["policy_role"] == "active_reference", "policy_id"
    ].unique()
    if len(active_ids) != 1:
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary must contain exactly one active-reference policy."
        )
    if active_ids[0] != "active-reference":
        raise IntervalPolicyReviewDecisionError(
            "The active-reference policy must use policy_id active-reference."
        )
    active = prepared.loc[prepared["policy_id"] == active_ids[0]]
    if not (
        active["candidate_status"].to_numpy()
        == active["active_reference_status"].to_numpy()
    ).all():
        raise IntervalPolicyReviewDecisionError(
            "Active-reference candidate status must equal active_reference_status."
        )
    if active["status_changed_from_active"].any():
        raise IntervalPolicyReviewDecisionError(
            "Active-reference rows cannot be marked changed from active."
        )
    expected_scenarios = set(active["scenario"])
    for policy_id, group in prepared.groupby("policy_id", sort=False):
        if set(group["scenario"]) != expected_scenarios:
            raise IntervalPolicyReviewDecisionError(
                f"Policy {policy_id} does not retain the complete scenario cohort."
            )
    return prepared.sort_values(["policy_id", "scenario"]).reset_index(drop=True)


def sensitivity_summary_sha256(summary: pd.DataFrame) -> str:
    prepared = prepare_sensitivity_summary(summary)
    return _digest(prepared.to_dict(orient="records"))


def create_interval_policy_review_decision(
    sensitivity_summary: pd.DataFrame,
    *,
    decision: str,
    target_policy_id: str,
    reviewer_name: str,
    reviewer_role: str,
    review_ticket: str,
    rationale: str,
    requested_revision: str | None = None,
    decided_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable named decision without activating policy thresholds."""
    prepared = prepare_sensitivity_summary(sensitivity_summary)
    decision = _required_text(decision, "decision")
    if decision not in ALLOWED_DECISIONS:
        raise IntervalPolicyReviewDecisionError(
            "decision must be retain_active_policy, reject_candidate, or request_revision."
        )
    target_policy_id = _required_text(target_policy_id, "target_policy_id")
    reviewer_name = _required_text(reviewer_name, "reviewer_name")
    reviewer_role = _required_text(reviewer_role, "reviewer_role")
    review_ticket = _required_text(review_ticket, "review_ticket")
    rationale = _required_text(rationale, "rationale")
    revision = _optional_text(requested_revision)
    target = prepared.loc[prepared["policy_id"] == target_policy_id]
    if target.empty:
        raise IntervalPolicyReviewDecisionError(
            f"target_policy_id {target_policy_id} is absent from sensitivity evidence."
        )
    target_role = str(target.iloc[0]["policy_role"])
    if decision == "retain_active_policy":
        if target_policy_id != "active-reference" or target_role != "active_reference":
            raise IntervalPolicyReviewDecisionError(
                "retain_active_policy must target the active-reference policy."
            )
        if revision is not None:
            raise IntervalPolicyReviewDecisionError(
                "retain_active_policy cannot include a requested revision."
            )
    elif decision == "reject_candidate":
        if target_role != "review_candidate" or target_policy_id == "active-reference":
            raise IntervalPolicyReviewDecisionError(
                "reject_candidate must target a review candidate."
            )
        if revision is not None:
            raise IntervalPolicyReviewDecisionError(
                "reject_candidate cannot include a requested revision."
            )
    else:
        if target_role != "review_candidate" or target_policy_id == "active-reference":
            raise IntervalPolicyReviewDecisionError(
                "request_revision must target a review candidate."
            )
        if revision is None or len(revision) < 20:
            raise IntervalPolicyReviewDecisionError(
                "request_revision requires a requested_revision of at least 20 characters."
            )

    sensitivity_timestamp = prepared.iloc[0]["sensitivity_run_timestamp_utc"]
    decided_at = _utc(decided_at_utc or datetime.now(timezone.utc), "decided_at_utc")
    if decided_at < sensitivity_timestamp:
        raise IntervalPolicyReviewDecisionError(
            "decided_at_utc cannot precede the sensitivity run timestamp."
        )
    active = prepared.loc[prepared["policy_id"] == "active-reference"].iloc[0]
    scenario_evidence = []
    for _, row in target.sort_values("scenario").iterrows():
        scenario_evidence.append(
            {
                "scenario": row["scenario"],
                "retained_monitor_status": row["retained_monitor_status"],
                "active_reference_status": row["active_reference_status"],
                "target_candidate_status": row["candidate_status"],
                "sensitivity_classification": row["sensitivity_classification"],
                "status_changed_from_active": bool(row["status_changed_from_active"]),
                "changed_slice_count": int(row["changed_slice_count"]),
                "human_review_required": bool(row["human_review_required"]),
            }
        )
    core = {
        "sensitivity_run_id": prepared.iloc[0]["sensitivity_run_id"],
        "sensitivity_run_timestamp_utc": sensitivity_timestamp.isoformat(),
        "trend_run_id": prepared.iloc[0]["trend_run_id"],
        "sensitivity_summary_sha256": sensitivity_summary_sha256(sensitivity_summary),
        "active_policy_id": "active-reference",
        "active_policy_version": active["policy_version"],
        "target_policy_id": target_policy_id,
        "target_policy_role": target_role,
        "target_policy_version": target.iloc[0]["policy_version"],
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "reviewer_name": reviewer_name,
        "reviewer_role": reviewer_role,
        "review_ticket": review_ticket,
        "rationale": rationale,
        "requested_revision": revision,
        "decided_at_utc": decided_at.isoformat(),
        "scenario_evidence": scenario_evidence,
    }
    result = {
        "decision_id": "ipd-" + _digest(core)[:24],
        "decision_revision": 1,
        **core,
        "human_decision_confirmed": True,
        "follow_up_human_action_required": True,
        "threshold_activation_authorized": False,
        "candidate_thresholds_activated": False,
        "active_policy_updated": False,
        "retained_evidence_mutated": False,
        "interval_recalibration_performed": False,
        "model_change_performed": False,
        "schedule_change_performed": False,
        "promotion_change_performed": False,
        "alert_delivery_performed": False,
        "deployment_performed": False,
        "external_publication_performed": False,
        "decision_contract_version": DECISION_CONTRACT_VERSION,
    }
    result["decision_hash"] = _digest(result)
    verify_interval_policy_review_decision(result, sensitivity_summary)
    return result


def verify_interval_policy_review_decision(
    decision: dict[str, Any], sensitivity_summary: pd.DataFrame | None = None
) -> None:
    """Verify one standalone or evidence-bound named policy decision."""
    if not isinstance(decision, dict):
        raise IntervalPolicyReviewDecisionError("Decision must be a JSON object.")
    if decision.get("decision_contract_version") != DECISION_CONTRACT_VERSION:
        raise IntervalPolicyReviewDecisionError("Unsupported decision contract version.")
    if not DECISION_ID_PATTERN.fullmatch(
        _required_text(decision.get("decision_id"), "decision_id")
    ):
        raise IntervalPolicyReviewDecisionError("decision_id is malformed.")
    if decision.get("decision_revision") != 1:
        raise IntervalPolicyReviewDecisionError("decision_revision must be 1.")
    stored_hash = _required_text(decision.get("decision_hash"), "decision_hash")
    if not SHA256_PATTERN.fullmatch(stored_hash):
        raise IntervalPolicyReviewDecisionError("decision_hash is malformed.")
    material = dict(decision)
    material.pop("decision_hash", None)
    if stored_hash != _digest(material):
        raise IntervalPolicyReviewDecisionError("Decision hash is invalid.")
    if decision.get("decision") not in ALLOWED_DECISIONS:
        raise IntervalPolicyReviewDecisionError("Decision outcome is invalid.")
    if decision.get("decision_effect") != DECISION_EFFECTS[decision["decision"]]:
        raise IntervalPolicyReviewDecisionError("Decision effect is inconsistent.")
    for field in (
        "reviewer_name", "reviewer_role", "review_ticket", "rationale",
        "sensitivity_run_id", "trend_run_id", "target_policy_id",
        "target_policy_role", "target_policy_version", "active_policy_id",
        "active_policy_version",
    ):
        _required_text(decision.get(field), field)
    _utc(decision.get("decided_at_utc"), "decided_at_utc")
    _utc(decision.get("sensitivity_run_timestamp_utc"), "sensitivity_run_timestamp_utc")
    if not SHA256_PATTERN.fullmatch(
        _required_text(decision.get("sensitivity_summary_sha256"), "sensitivity_summary_sha256")
    ):
        raise IntervalPolicyReviewDecisionError("sensitivity_summary_sha256 is malformed.")
    scenarios = decision.get("scenario_evidence")
    if not isinstance(scenarios, list) or not scenarios:
        raise IntervalPolicyReviewDecisionError(
            "scenario_evidence must contain retained scenario rows."
        )
    names = []
    for row in scenarios:
        if not isinstance(row, dict):
            raise IntervalPolicyReviewDecisionError(
                "scenario_evidence rows must be objects."
            )
        names.append(_required_text(row.get("scenario"), "scenario"))
        for field in (
            "retained_monitor_status", "active_reference_status", "target_candidate_status"
        ):
            if row.get(field) not in SUPPORTED_STATUSES:
                raise IntervalPolicyReviewDecisionError(f"Scenario {field} is invalid.")
        if row.get("sensitivity_classification") not in SUPPORTED_CLASSIFICATIONS:
            raise IntervalPolicyReviewDecisionError(
                "Scenario sensitivity classification is invalid."
            )
        _bool(row.get("status_changed_from_active"), "status_changed_from_active")
        _bool(row.get("human_review_required"), "human_review_required")
        changed = row.get("changed_slice_count")
        if isinstance(changed, bool) or not isinstance(changed, int) or changed < 0:
            raise IntervalPolicyReviewDecisionError(
                "changed_slice_count must be a non-negative integer."
            )
    if len(names) != len(set(names)):
        raise IntervalPolicyReviewDecisionError(
            "scenario_evidence must not contain duplicate scenarios."
        )
    outcome = decision["decision"]
    revision = _optional_text(decision.get("requested_revision"))
    if outcome == "retain_active_policy":
        if (
            decision.get("target_policy_id") != "active-reference"
            or decision.get("target_policy_role") != "active_reference"
            or revision is not None
        ):
            raise IntervalPolicyReviewDecisionError("Retain decision fields are inconsistent.")
    elif outcome == "reject_candidate":
        if (
            decision.get("target_policy_role") != "review_candidate"
            or decision.get("target_policy_id") == "active-reference"
            or revision is not None
        ):
            raise IntervalPolicyReviewDecisionError("Reject decision fields are inconsistent.")
    elif (
        decision.get("target_policy_role") != "review_candidate"
        or decision.get("target_policy_id") == "active-reference"
        or revision is None
        or len(revision) < 20
    ):
        raise IntervalPolicyReviewDecisionError("Revision decision fields are inconsistent.")
    safety = {
        "human_decision_confirmed": True,
        "follow_up_human_action_required": True,
        "threshold_activation_authorized": False,
        "candidate_thresholds_activated": False,
        "active_policy_updated": False,
        "retained_evidence_mutated": False,
        "interval_recalibration_performed": False,
        "model_change_performed": False,
        "schedule_change_performed": False,
        "promotion_change_performed": False,
        "alert_delivery_performed": False,
        "deployment_performed": False,
        "external_publication_performed": False,
    }
    for field, expected in safety.items():
        if decision.get(field) is not expected:
            raise IntervalPolicyReviewDecisionError(
                f"Decision safety field {field} is invalid."
            )
    if sensitivity_summary is not None:
        prepared = prepare_sensitivity_summary(sensitivity_summary)
        if decision.get("sensitivity_run_id") != prepared.iloc[0]["sensitivity_run_id"]:
            raise IntervalPolicyReviewDecisionError(
                "Decision sensitivity_run_id does not match supplied evidence."
            )
        if decision.get("sensitivity_summary_sha256") != sensitivity_summary_sha256(
            sensitivity_summary
        ):
            raise IntervalPolicyReviewDecisionError(
                "Decision sensitivity summary digest does not match supplied evidence."
            )
        target = prepared.loc[prepared["policy_id"] == decision.get("target_policy_id")]
        if target.empty:
            raise IntervalPolicyReviewDecisionError(
                "Decision target policy is absent from supplied evidence."
            )
        expected_scenarios = [
            {
                "scenario": row["scenario"],
                "retained_monitor_status": row["retained_monitor_status"],
                "active_reference_status": row["active_reference_status"],
                "target_candidate_status": row["candidate_status"],
                "sensitivity_classification": row["sensitivity_classification"],
                "status_changed_from_active": bool(row["status_changed_from_active"]),
                "changed_slice_count": int(row["changed_slice_count"]),
                "human_review_required": bool(row["human_review_required"]),
            }
            for _, row in target.sort_values("scenario").iterrows()
        ]
        if decision.get("scenario_evidence") != expected_scenarios:
            raise IntervalPolicyReviewDecisionError(
                "Decision scenario evidence does not match supplied sensitivity evidence."
            )


def render_interval_policy_review_decision(decision: dict[str, Any]) -> str:
    verify_interval_policy_review_decision(decision)
    lines = [
        "# Monitoring-policy review decision", "",
        f"- Decision ID: `{decision['decision_id']}`",
        f"- Sensitivity run: `{decision['sensitivity_run_id']}`",
        f"- Decision: **{decision['decision']}**",
        f"- Target policy: `{decision['target_policy_id']}`",
        f"- Reviewer: {decision['reviewer_name']} ({decision['reviewer_role']})",
        f"- Review ticket: `{decision['review_ticket']}`",
        f"- Decided at: `{decision['decided_at_utc']}`", "",
        "## Rationale", "", decision["rationale"],
    ]
    if decision.get("requested_revision"):
        lines.extend(["", "## Requested revision", "", decision["requested_revision"]])
    lines.extend([
        "", "## Retained scenario evidence", "",
        "| Scenario | Retained | Active reference | Target candidate | Classification | Changed slices |",
        "| --- | --- | --- | --- | --- | ---: |",
    ])
    for row in decision["scenario_evidence"]:
        lines.append(
            "| {scenario} | {retained_monitor_status} | {active_reference_status} | "
            "{target_candidate_status} | {sensitivity_classification} | "
            "{changed_slice_count} |".format(**row)
        )
    lines.extend([
        "", "## Authority boundary", "",
        "This receipt records human review evidence only. It does not activate candidate thresholds, update the active policy, recalibrate an interval, change a model or schedule, promote anything, deliver an alert, deploy, or publish externally.", "",
    ])
    return "\n".join(lines)


def write_interval_policy_review_decision(
    output_directory: Path, decision: dict[str, Any]
) -> tuple[Path, Path]:
    verify_interval_policy_review_decision(decision)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / f"interval_policy_review_decision_{decision['decision_id']}.json"
    markdown_path = output_directory / f"interval_policy_review_decision_{decision['decision_id']}.md"
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    for candidate in (json_path, markdown_path, temporary_json, temporary_markdown):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary_json.write_text(
            json.dumps(_canonical(decision), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(
            render_interval_policy_review_decision(decision), encoding="utf-8"
        )
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return json_path, markdown_path


def read_sensitivity_summary(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.casefold() == ".csv":
        return pd.read_csv(path)
    if path.suffix.casefold() == ".parquet":
        return pd.read_parquet(path)
    raise IntervalPolicyReviewDecisionError(
        "Sensitivity summary must be CSV or Parquet."
    )
