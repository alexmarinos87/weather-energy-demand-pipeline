from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4
import hashlib
import json
import re

import pandas as pd

from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_review_decision import verify_policy_review_decision


REVISION_CONTRACT_VERSION = "interval-policy-candidate-revision-v1"
REVISION_ID_PATTERN = re.compile(r"^ipcr-[0-9a-f]{24}$")
AUTHORITY_FIELDS = (
    "threshold_activation_authorized",
    "candidate_thresholds_activated",
    "active_policy_updated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyCandidateRevisionError(ValueError):
    """Raised when a candidate revision package is malformed or unsafe."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise IntervalPolicyCandidateRevisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise IntervalPolicyCandidateRevisionError(f"{name} must be non-empty.")
    return text


def _unique_texts(values: Iterable[Any], name: str) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyCandidateRevisionError(f"{name} must be a list of strings.")
    result = [_required_text(value, name) for value in values]
    if not result:
        raise IntervalPolicyCandidateRevisionError(f"{name} must contain at least one item.")
    if len(set(result)) != len(result):
        raise IntervalPolicyCandidateRevisionError(f"{name} must not contain duplicates.")
    return result


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyCandidateRevisionError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_timestamp(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalise_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(policy, Mapping):
        raise IntervalPolicyCandidateRevisionError(
            "proposed_policy must be a JSON object."
        )
    active = asdict(PredictionIntervalMonitoringConfig())
    expected = set(active)
    supplied = set(policy)
    missing = sorted(expected - supplied)
    unexpected = sorted(supplied - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        raise IntervalPolicyCandidateRevisionError(
            "proposed_policy fields are invalid: " + "; ".join(details) + "."
        )
    try:
        config = PredictionIntervalMonitoringConfig(**dict(policy))
        config.validate()
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"proposed_policy is invalid: {exc}"
        ) from exc
    return asdict(config)


def _decision_timestamp(decision: Mapping[str, Any]) -> pd.Timestamp:
    for name in ("decision_timestamp_utc", "decided_at_utc"):
        if decision.get(name) is not None:
            return _utc_timestamp(decision[name], name)
    raise IntervalPolicyCandidateRevisionError(
        "Source decision does not contain a decision timestamp."
    )


def _decision_hash(decision: Mapping[str, Any]) -> str:
    for name in ("decision_sha256", "decision_hash"):
        value = decision.get(name)
        if value:
            return _required_text(value, name)
    raise IntervalPolicyCandidateRevisionError(
        "Source decision does not contain a decision hash."
    )


def _target_candidate_id(decision: Mapping[str, Any]) -> str:
    for name in ("target_candidate_id", "target_policy_id"):
        value = decision.get(name)
        if value:
            return _required_text(value, name)
    raise IntervalPolicyCandidateRevisionError(
        "Source decision does not identify its target candidate."
    )


def create_candidate_revision_package(
    decision: dict[str, Any],
    sensitivity_summary: pd.DataFrame,
    *,
    proposed_policy: Mapping[str, Any],
    revised_candidate_id: str,
    revised_candidate_version: str,
    proposed_by: str,
    proposer_role: str,
    revision_ticket: str,
    rationale: str,
    evidence_notes: Iterable[Any],
    created_at_utc: Any | None = None,
    revision_id: str | None = None,
) -> dict[str, Any]:
    """Create one immutable candidate revision package without activation authority."""
    try:
        verify_policy_review_decision(decision, sensitivity_summary)
    except Exception as exc:  # normalise the dependency boundary
        raise IntervalPolicyCandidateRevisionError(
            f"Source policy-review decision failed verification: {exc}"
        ) from exc

    if decision.get("decision") not in {
        "request_candidate_revision",
        "request_revision",
    }:
        raise IntervalPolicyCandidateRevisionError(
            "A candidate revision package requires a request-revision decision."
        )
    if decision.get("target_candidate_role") != "review_candidate":
        raise IntervalPolicyCandidateRevisionError(
            "A candidate revision package must target a review candidate."
        )

    source_candidate_id = _target_candidate_id(decision)
    revised_id = _required_text(revised_candidate_id, "revised_candidate_id")
    if revised_id in {source_candidate_id, "active-reference"}:
        raise IntervalPolicyCandidateRevisionError(
            "revised_candidate_id must identify a new non-active candidate revision."
        )
    revised_version = _required_text(
        revised_candidate_version, "revised_candidate_version"
    )
    actor = _required_text(proposed_by, "proposed_by")
    role = _required_text(proposer_role, "proposer_role")
    ticket = _required_text(revision_ticket, "revision_ticket")
    reason = _required_text(rationale, "rationale")
    notes = _unique_texts(evidence_notes, "evidence_notes")
    requested_changes = _unique_texts(
        decision.get("requested_changes", ()), "source requested_changes"
    )
    policy = _normalise_policy(proposed_policy)
    active_policy = asdict(PredictionIntervalMonitoringConfig())
    changes = [
        {
            "field": key,
            "active_value": active_policy[key],
            "proposed_value": policy[key],
        }
        for key in sorted(policy)
        if policy[key] != active_policy[key]
    ]
    if not changes:
        raise IntervalPolicyCandidateRevisionError(
            "The revised candidate must differ from the checked-in active policy."
        )

    created_at = _utc_timestamp(
        created_at_utc or datetime.now(timezone.utc), "created_at_utc"
    )
    if created_at < _decision_timestamp(decision):
        raise IntervalPolicyCandidateRevisionError(
            "created_at_utc cannot precede the source human decision."
        )
    identifier = revision_id or "ipcr-" + uuid4().hex[:24]
    if not REVISION_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyCandidateRevisionError(
            "revision_id must be ipcr- plus 24 lowercase hexadecimal characters."
        )

    package = {
        "revision_id": identifier,
        "revision_number": 1,
        "created_at_utc": created_at.isoformat(),
        "source_decision_id": _required_text(
            decision.get("decision_id"), "source decision_id"
        ),
        "source_decision_sha256": _decision_hash(decision),
        "source_decision": decision["decision"],
        "source_sensitivity_run_id": _required_text(
            decision.get("sensitivity_run_id"), "source sensitivity_run_id"
        ),
        "source_sensitivity_summary_sha256": _required_text(
            decision.get("sensitivity_summary_sha256"),
            "source sensitivity_summary_sha256",
        ),
        "source_candidate_id": source_candidate_id,
        "source_candidate_version": _required_text(
            decision.get("target_candidate_version"),
            "source target_candidate_version",
        ),
        "revised_candidate_id": revised_id,
        "revised_candidate_version": revised_version,
        "requested_changes": requested_changes,
        "evidence_notes": notes,
        "proposed_policy": policy,
        "proposed_policy_sha256": _digest(policy),
        "active_policy_snapshot": active_policy,
        "active_policy_sha256": _digest(active_policy),
        "changed_thresholds": changes,
        "changed_threshold_count": len(changes),
        "proposed_by": actor,
        "proposer_role": role,
        "revision_ticket": ticket,
        "rationale": reason,
        "human_review_required": True,
        "candidate_revision_contract_version": REVISION_CONTRACT_VERSION,
        **{field: False for field in AUTHORITY_FIELDS},
    }
    package["revision_sha256"] = _digest(package)
    verify_candidate_revision_package(package, decision, sensitivity_summary)
    return package


def verify_candidate_revision_package(
    package: dict[str, Any],
    decision: dict[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> None:
    """Verify a package and its complete binding to the G27 decision evidence."""
    if not isinstance(package, dict):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package must be a JSON object."
        )
    try:
        verify_policy_review_decision(decision, sensitivity_summary)
    except Exception as exc:
        raise IntervalPolicyCandidateRevisionError(
            f"Source policy-review decision failed verification: {exc}"
        ) from exc
    if package.get("candidate_revision_contract_version") != REVISION_CONTRACT_VERSION:
        raise IntervalPolicyCandidateRevisionError(
            "Unsupported candidate revision contract."
        )
    identifier = _required_text(package.get("revision_id"), "revision_id")
    if not REVISION_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyCandidateRevisionError("revision_id is malformed.")
    if package.get("revision_number") != 1:
        raise IntervalPolicyCandidateRevisionError("revision_number must be 1.")
    retained_hash = package.get("revision_sha256")
    material = {key: value for key, value in package.items() if key != "revision_sha256"}
    if retained_hash != _digest(material):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package hash is invalid."
        )
    if package.get("source_decision_id") != decision.get("decision_id"):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package decision_id is inconsistent."
        )
    if package.get("source_decision_sha256") != _decision_hash(decision):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package decision hash is inconsistent."
        )
    if package.get("source_sensitivity_summary_sha256") != decision.get(
        "sensitivity_summary_sha256"
    ):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package sensitivity digest is inconsistent."
        )
    if package.get("source_candidate_id") != _target_candidate_id(decision):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision package target candidate is inconsistent."
        )
    policy = _normalise_policy(package.get("proposed_policy", {}))
    if package.get("proposed_policy_sha256") != _digest(policy):
        raise IntervalPolicyCandidateRevisionError(
            "Proposed policy digest is invalid."
        )
    active = asdict(PredictionIntervalMonitoringConfig())
    if package.get("active_policy_snapshot") != active:
        raise IntervalPolicyCandidateRevisionError(
            "Active policy snapshot no longer matches the checked-in policy."
        )
    if package.get("active_policy_sha256") != _digest(active):
        raise IntervalPolicyCandidateRevisionError(
            "Active policy digest is invalid."
        )
    expected_changes = [
        {
            "field": key,
            "active_value": active[key],
            "proposed_value": policy[key],
        }
        for key in sorted(policy)
        if policy[key] != active[key]
    ]
    if package.get("changed_thresholds") != expected_changes:
        raise IntervalPolicyCandidateRevisionError(
            "Changed-threshold evidence is inconsistent."
        )
    if package.get("changed_threshold_count") != len(expected_changes) or not expected_changes:
        raise IntervalPolicyCandidateRevisionError(
            "Changed-threshold count is invalid."
        )
    _unique_texts(package.get("requested_changes", ()), "requested_changes")
    _unique_texts(package.get("evidence_notes", ()), "evidence_notes")
    _required_text(package.get("proposed_by"), "proposed_by")
    _required_text(package.get("proposer_role"), "proposer_role")
    _required_text(package.get("revision_ticket"), "revision_ticket")
    _required_text(package.get("rationale"), "rationale")
    if _utc_timestamp(package.get("created_at_utc"), "created_at_utc") < _decision_timestamp(decision):
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision predates the source decision."
        )
    if package.get("human_review_required") is not True:
        raise IntervalPolicyCandidateRevisionError(
            "Candidate revision must require further human review."
        )
    for field in AUTHORITY_FIELDS:
        if package.get(field) is not False:
            raise IntervalPolicyCandidateRevisionError(
                f"Candidate revision safety field {field} must be false."
            )


def render_candidate_revision_markdown(package: dict[str, Any]) -> str:
    lines = [
        "# Interval-policy candidate revision package",
        "",
        f"- Revision ID: `{package['revision_id']}`",
        f"- Source decision: `{package['source_decision_id']}`",
        f"- Source candidate: `{package['source_candidate_id']}`",
        f"- Revised candidate: `{package['revised_candidate_id']}`",
        f"- Proposed by: {package['proposed_by']} ({package['proposer_role']})",
        f"- Revision ticket: `{package['revision_ticket']}`",
        "",
        "## Requested changes",
        "",
    ]
    lines.extend(f"- {item}" for item in package["requested_changes"])
    lines += ["", "## Changed thresholds", "", "| Field | Active | Proposed |", "| --- | ---: | ---: |"]
    for change in package["changed_thresholds"]:
        lines.append(
            f"| `{change['field']}` | {change['active_value']} | {change['proposed_value']} |"
        )
    lines += [
        "",
        "## Authority boundary",
        "",
        "This package is review evidence only. It does not activate thresholds, update the active policy, recalibrate intervals, change a model or schedule, deliver an alert, promote a candidate, deploy, or publish externally.",
        "",
    ]
    return "\n".join(lines)


def write_candidate_revision_package(
    output_directory: Path,
    package: dict[str, Any],
) -> tuple[Path, Path]:
    """Write immutable JSON and Markdown evidence without overwriting prior packages."""
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / f"interval_policy_candidate_revision_{package['revision_id']}.json"
    markdown_path = output_directory / f"interval_policy_candidate_revision_{package['revision_id']}.md"
    for path in (json_path, markdown_path):
        if path.exists() or path.with_suffix(path.suffix + ".tmp").exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")
    markdown_tmp = markdown_path.with_suffix(markdown_path.suffix + ".tmp")
    try:
        json_tmp.write_text(
            json.dumps(_canonical(package), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        markdown_tmp.write_text(
            render_candidate_revision_markdown(package), encoding="utf-8"
        )
        json_tmp.replace(json_path)
        markdown_tmp.replace(markdown_path)
    finally:
        json_tmp.unlink(missing_ok=True)
        markdown_tmp.unlink(missing_ok=True)
    return json_path, markdown_path
