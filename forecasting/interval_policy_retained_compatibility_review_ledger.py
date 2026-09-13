from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping
from uuid import uuid4

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import (
    canonical,
    digest,
    utc_timestamp,
)
from forecasting.interval_policy_retained_compatibility_review import (
    ALLOWED_DECISIONS,
    DECISION_EFFECTS,
    REVIEW_CONTRACT_VERSION,
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityReviewError,
    verify_retained_compatibility_review,
)


LEDGER_CONTRACT_VERSION = (
    "interval-policy-retained-compatibility-review-ledger-v1"
)
LEDGER_ID_PATTERN = re.compile(r"^ipcrl-[0-9a-f]{24}$")
LEDGER_SAFETY_FIELDS = (
    "source_reviews_mutated",
    "source_compatibility_evidence_mutated",
    "historical_statuses_rewritten",
    "historical_annotation_applied",
    "retained_evidence_mutated",
    "monitoring_rerun_performed",
    "policy_version_updated",
    "threshold_activation_performed",
    "interval_recalibration_performed",
    "model_change_performed",
    "fabric_execution_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)
ENTRY_REQUIRED_COLUMNS = {
    "ledger_run_id",
    "ledger_run_timestamp_utc",
    "review_sequence",
    "review_id",
    "review_sha256",
    "reviewed_at_utc",
    "compatibility_run_id",
    "compatibility_summary_sha256",
    "compatibility_manifest_sha256",
    "trend_run_id",
    "decision",
    "decision_effect",
    "reviewer_name",
    "reviewer_role",
    "review_ticket",
    "follow_up_human_action_required",
    "requested_action_count",
    "scenario_count",
    "changed_slice_count",
    "newly_failed_slice_count",
    "review_contract_version",
    "ledger_contract_version",
    *LEDGER_SAFETY_FIELDS,
}
SUMMARY_REQUIRED_COLUMNS = {
    "ledger_run_id",
    "ledger_run_timestamp_utc",
    "review_count",
    "compatibility_run_count",
    "accepted_transition_count",
    "historical_annotation_request_count",
    "compatibility_reassessment_request_count",
    "follow_up_review_count",
    "earliest_reviewed_at_utc",
    "latest_reviewed_at_utc",
    "conflict_count",
    "human_review_required",
    "ledger_sha256",
    "ledger_contract_version",
    *LEDGER_SAFETY_FIELDS,
}


class IntervalPolicyRetainedCompatibilityReviewLedgerError(ValueError):
    """Raised when retained compatibility-review ledger evidence is invalid."""


def _required_text(value: Any, name: str) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            f"{name} must be non-empty."
        )
    return text


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        return utc_timestamp(value, name)
    except Exception as exc:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(str(exc)) from exc


def _verify_source_review(
    review: Mapping[str, Any],
    summary: pd.DataFrame,
    manifest: Mapping[str, Any],
    artifact_directory: Path,
) -> None:
    try:
        verify_retained_compatibility_review(
            review,
            summary,
            manifest,
            artifact_directory=artifact_directory,
        )
    except IntervalPolicyRetainedCompatibilityReviewError as exc:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(str(exc)) from exc


def _ledger_digest(entries: pd.DataFrame, summary: Mapping[str, Any]) -> str:
    summary_without_hash = {
        key: value for key, value in summary.items() if key != "ledger_sha256"
    }
    return digest(
        {
            "entries": canonical(entries.to_dict(orient="records")),
            "summary": canonical(summary_without_hash),
        }
    )


def build_retained_compatibility_review_ledger(
    bindings: Iterable[
        tuple[Mapping[str, Any], pd.DataFrame, Mapping[str, Any], Path]
    ],
    *,
    ledger_run_id: str | None = None,
    ledger_run_timestamp_utc: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build one ordered append-only ledger from independently verified reviews."""
    materialized = list(bindings)
    if not materialized:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "At least one retained compatibility review binding is required."
        )

    identifier = ledger_run_id or "ipcrl-" + uuid4().hex[:24]
    if not LEDGER_ID_PATTERN.fullmatch(identifier):
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "ledger_run_id must be ipcrl- plus 24 lowercase hexadecimal characters."
        )
    ledger_timestamp = _utc(
        ledger_run_timestamp_utc or datetime.now(timezone.utc),
        "ledger_run_timestamp_utc",
    )

    rows: list[dict[str, Any]] = []
    review_ids: set[str] = set()
    compatibility_runs: dict[str, str] = {}
    for review, summary, manifest, artifact_directory in materialized:
        _verify_source_review(review, summary, manifest, Path(artifact_directory))
        document = dict(review)
        review_id = _required_text(document.get("review_id"), "review_id")
        if review_id in review_ids:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                f"Duplicate compatibility review ID: {review_id}."
            )
        review_ids.add(review_id)
        run_id = _required_text(
            document.get("compatibility_run_id"), "compatibility_run_id"
        )
        if run_id in compatibility_runs:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Conflicting compatibility reviews target the same "
                f"compatibility run: {run_id}."
            )
        compatibility_runs[run_id] = review_id

        reviewed_at = _utc(document.get("reviewed_at_utc"), "reviewed_at_utc")
        if reviewed_at > ledger_timestamp:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "The ledger timestamp cannot precede a retained review."
            )
        decision = _required_text(document.get("decision"), "decision")
        if decision not in ALLOWED_DECISIONS:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Retained review decision is unsupported."
            )
        if document.get("decision_effect") != DECISION_EFFECTS[decision]:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Retained review decision effect is inconsistent."
            )
        if document.get("review_contract_version") != REVIEW_CONTRACT_VERSION:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Retained review contract version is invalid."
            )
        for field in REVIEW_SAFETY_FIELDS:
            if document.get(field) is not False:
                raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                    f"Retained review safety field {field} must be false."
                )
        scenarios = document.get("scenario_evidence")
        if not isinstance(scenarios, list) or not scenarios:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Retained review requires scenario evidence."
            )
        actions = document.get("requested_actions")
        if not isinstance(actions, list):
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Retained review requested_actions must be a list."
            )
        rows.append(
            {
                "ledger_run_id": identifier,
                "ledger_run_timestamp_utc": ledger_timestamp,
                "review_id": review_id,
                "review_sha256": _required_text(
                    document.get("review_sha256"), "review_sha256"
                ),
                "reviewed_at_utc": reviewed_at,
                "compatibility_run_id": run_id,
                "compatibility_summary_sha256": _required_text(
                    document.get("compatibility_summary_sha256"),
                    "compatibility_summary_sha256",
                ),
                "compatibility_manifest_sha256": _required_text(
                    document.get("compatibility_manifest_sha256"),
                    "compatibility_manifest_sha256",
                ),
                "trend_run_id": _required_text(
                    document.get("trend_run_id"), "trend_run_id"
                ),
                "decision": decision,
                "decision_effect": document["decision_effect"],
                "reviewer_name": _required_text(
                    document.get("reviewer_name"), "reviewer_name"
                ),
                "reviewer_role": _required_text(
                    document.get("reviewer_role"), "reviewer_role"
                ),
                "review_ticket": _required_text(
                    document.get("review_ticket"), "review_ticket"
                ),
                "follow_up_human_action_required": bool(
                    document.get("follow_up_human_action_required")
                ),
                "requested_action_count": len(actions),
                "scenario_count": len(scenarios),
                "changed_slice_count": sum(
                    int(item["changed_slice_count"]) for item in scenarios
                ),
                "newly_failed_slice_count": sum(
                    int(item["newly_failed_slice_count"]) for item in scenarios
                ),
                "review_contract_version": document["review_contract_version"],
                "ledger_contract_version": LEDGER_CONTRACT_VERSION,
                **{field: False for field in LEDGER_SAFETY_FIELDS},
            }
        )

    entries = pd.DataFrame(rows).sort_values(
        ["reviewed_at_utc", "review_id"]
    ).reset_index(drop=True)
    entries.insert(2, "review_sequence", range(1, len(entries) + 1))
    decisions = entries["decision"]
    summary_row: dict[str, Any] = {
        "ledger_run_id": identifier,
        "ledger_run_timestamp_utc": ledger_timestamp,
        "review_count": int(len(entries)),
        "compatibility_run_count": int(entries["compatibility_run_id"].nunique()),
        "accepted_transition_count": int(
            (decisions == "accept_non_retroactive_transition").sum()
        ),
        "historical_annotation_request_count": int(
            (decisions == "request_historical_annotation").sum()
        ),
        "compatibility_reassessment_request_count": int(
            (decisions == "request_compatibility_reassessment").sum()
        ),
        "follow_up_review_count": int(
            entries["follow_up_human_action_required"].sum()
        ),
        "earliest_reviewed_at_utc": entries["reviewed_at_utc"].min(),
        "latest_reviewed_at_utc": entries["reviewed_at_utc"].max(),
        "conflict_count": 0,
        "human_review_required": bool(
            entries["follow_up_human_action_required"].any()
        ),
        "ledger_contract_version": LEDGER_CONTRACT_VERSION,
        **{field: False for field in LEDGER_SAFETY_FIELDS},
    }
    summary_row["ledger_sha256"] = _ledger_digest(entries, summary_row)
    summary = pd.DataFrame([summary_row])
    verify_retained_compatibility_review_ledger(entries, summary)
    return entries, summary


def verify_retained_compatibility_review_ledger(
    entries: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    """Verify the internal order, counts, hashes, and authority boundary."""
    missing_entries = sorted(ENTRY_REQUIRED_COLUMNS - set(entries.columns))
    missing_summary = sorted(SUMMARY_REQUIRED_COLUMNS - set(summary.columns))
    if missing_entries or missing_summary:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger evidence is missing required columns."
        )
    if entries.empty or len(summary) != 1:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger requires entries and exactly one summary row."
        )
    ledger_ids = set(entries["ledger_run_id"].astype(str))
    if len(ledger_ids) != 1 or summary["ledger_run_id"].iloc[0] not in ledger_ids:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger run identity is inconsistent."
        )
    ledger_id = next(iter(ledger_ids))
    if not LEDGER_ID_PATTERN.fullmatch(ledger_id):
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger run ID is invalid."
        )
    if entries["review_id"].duplicated().any():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger contains duplicate review IDs."
        )
    if entries["compatibility_run_id"].duplicated().any():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger contains conflicting compatibility-run decisions."
        )
    expected_sequence = list(range(1, len(entries) + 1))
    if entries["review_sequence"].astype(int).tolist() != expected_sequence:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger review sequence must be contiguous."
        )
    reviewed = pd.to_datetime(entries["reviewed_at_utc"], utc=True, errors="raise")
    if not reviewed.is_monotonic_increasing:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger reviews must be ordered chronologically."
        )
    ledger_timestamps = pd.to_datetime(
        entries["ledger_run_timestamp_utc"], utc=True, errors="raise"
    )
    summary_timestamp = _utc(
        summary["ledger_run_timestamp_utc"].iloc[0],
        "ledger_run_timestamp_utc",
    )
    if ledger_timestamps.nunique() != 1 or ledger_timestamps.iloc[0] != summary_timestamp:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger timestamps are inconsistent."
        )
    if summary_timestamp < reviewed.max():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger timestamp cannot precede retained reviews."
        )
    if set(entries["decision"]) - ALLOWED_DECISIONS:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger contains an unsupported decision."
        )
    for row in entries.itertuples(index=False):
        if row.decision_effect != DECISION_EFFECTS[row.decision]:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Ledger contains an inconsistent decision effect."
            )
    if not (entries["review_contract_version"] == REVIEW_CONTRACT_VERSION).all():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger contains an invalid review contract."
        )
    if not (entries["ledger_contract_version"] == LEDGER_CONTRACT_VERSION).all():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger entry contract version is invalid."
        )
    row = summary.iloc[0]
    expected_counts = {
        "review_count": len(entries),
        "compatibility_run_count": entries["compatibility_run_id"].nunique(),
        "accepted_transition_count": (
            entries["decision"] == "accept_non_retroactive_transition"
        ).sum(),
        "historical_annotation_request_count": (
            entries["decision"] == "request_historical_annotation"
        ).sum(),
        "compatibility_reassessment_request_count": (
            entries["decision"] == "request_compatibility_reassessment"
        ).sum(),
        "follow_up_review_count": entries[
            "follow_up_human_action_required"
        ].sum(),
        "conflict_count": 0,
    }
    for field, expected in expected_counts.items():
        if int(row[field]) != int(expected):
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                f"Ledger summary {field} is inconsistent."
            )
    if bool(row["human_review_required"]) is not bool(
        entries["follow_up_human_action_required"].any()
    ):
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger human-review status is inconsistent."
        )
    if _utc(row["earliest_reviewed_at_utc"], "earliest_reviewed_at_utc") != reviewed.min():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger earliest review timestamp is inconsistent."
        )
    if _utc(row["latest_reviewed_at_utc"], "latest_reviewed_at_utc") != reviewed.max():
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger latest review timestamp is inconsistent."
        )
    if row["ledger_contract_version"] != LEDGER_CONTRACT_VERSION:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger summary contract version is invalid."
        )
    for field in LEDGER_SAFETY_FIELDS:
        if not (entries[field] == False).all() or bool(row[field]):  # noqa: E712
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                f"Ledger safety field {field} must be false."
            )
    expected_hash = _ledger_digest(entries, row.to_dict())
    if row["ledger_sha256"] != expected_hash:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Ledger SHA-256 is invalid."
        )


def render_retained_compatibility_review_ledger(
    entries: pd.DataFrame,
    summary: pd.DataFrame,
) -> str:
    verify_retained_compatibility_review_ledger(entries, summary)
    row = summary.iloc[0]
    lines = [
        "# Retained interval-policy compatibility review ledger",
        "",
        f"- Ledger run: `{row['ledger_run_id']}`",
        f"- Reviews: {int(row['review_count'])}",
        f"- Compatibility assessments: {int(row['compatibility_run_count'])}",
        f"- Follow-up reviews: {int(row['follow_up_review_count'])}",
        f"- Human follow-up required: {bool(row['human_review_required'])}",
        "",
        "| Sequence | Compatibility run | Decision | Reviewer | Ticket | Reviewed at |",
        "| ---: | --- | --- | --- | --- | --- |",
    ]
    for item in entries.itertuples(index=False):
        lines.append(
            f"| {item.review_sequence} | {item.compatibility_run_id} | "
            f"{item.decision} | {item.reviewer_name} | {item.review_ticket} | "
            f"{pd.Timestamp(item.reviewed_at_utc).isoformat()} |"
        )
    lines.extend(
        [
            "",
            "The ledger is append-only evidence. It rejects duplicate review IDs and "
            "conflicting decisions for one compatibility assessment.",
            "It does not rewrite historical statuses, apply annotations, rerun "
            "monitoring, activate thresholds or schedules, execute Fabric, "
            "recalibrate intervals, alter models, deliver alerts, deploy, or "
            "publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_frame_atomic(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        if output_format == "csv":
            frame.to_csv(temporary, index=False)
        elif output_format == "parquet":
            frame.to_parquet(temporary, index=False)
        else:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "output_format must be csv or parquet."
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_retained_compatibility_review_ledger(
    output_directory: Path,
    entries: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_format: str = "parquet",
) -> dict[str, Path]:
    """Write immutable tabular, report, and self-binding manifest evidence."""
    verify_retained_compatibility_review_ledger(entries, summary)
    directory = Path(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    run_id = summary["ledger_run_id"].iloc[0]
    extension = "csv" if output_format == "csv" else "parquet"
    outputs = {
        "entries": directory
        / f"interval_policy_retained_compatibility_review_ledger_{run_id}.{extension}",
        "summary": directory
        / f"interval_policy_retained_compatibility_review_ledger_summary_{run_id}.{extension}",
        "report": directory
        / f"interval_policy_retained_compatibility_review_ledger_{run_id}.md",
        "manifest": directory
        / f"interval_policy_retained_compatibility_review_ledger_{run_id}.json",
    }
    temporary = {
        key: path.with_name(f".{path.name}.tmp") for key, path in outputs.items()
    }
    for path in (*outputs.values(), *temporary.values()):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        _write_frame_atomic(entries, outputs["entries"], output_format)
        _write_frame_atomic(summary, outputs["summary"], output_format)
        temporary["report"].write_text(
            render_retained_compatibility_review_ledger(entries, summary),
            encoding="utf-8",
        )
        manifest = {
            "ledger_run_id": run_id,
            "ledger_run_timestamp_utc": canonical(
                summary["ledger_run_timestamp_utc"].iloc[0]
            ),
            "ledger_contract_version": LEDGER_CONTRACT_VERSION,
            "ledger_sha256": summary["ledger_sha256"].iloc[0],
            "entries_path": outputs["entries"].name,
            "summary_path": outputs["summary"].name,
            "report_path": outputs["report"].name,
            **{field: False for field in LEDGER_SAFETY_FIELDS},
        }
        manifest["manifest_sha256"] = digest(manifest)
        temporary["manifest"].write_text(
            json.dumps(canonical(manifest), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary["report"].replace(outputs["report"])
        temporary["manifest"].replace(outputs["manifest"])
    except Exception:
        for path in outputs.values():
            path.unlink(missing_ok=True)
        raise
    finally:
        for path in temporary.values():
            path.unlink(missing_ok=True)
    return outputs


def read_review_bindings(
    binding_manifest_path: Path,
) -> list[tuple[dict[str, Any], pd.DataFrame, dict[str, Any], Path]]:
    """Load explicit review/source bindings without directory discovery."""
    path = Path(binding_manifest_path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Review binding manifest must be readable JSON."
        ) from exc
    bindings = document.get("bindings") if isinstance(document, dict) else None
    if not isinstance(bindings, list) or not bindings:
        raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
            "Review binding manifest requires a non-empty bindings array."
        )
    loaded = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in bindings:
        if not isinstance(item, dict):
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Each review binding must be a JSON object."
            )
        names = {}
        for field in (
            "review",
            "compatibility_summary",
            "compatibility_manifest",
            "artifact_directory",
        ):
            names[field] = _required_text(item.get(field), field)
        identity = tuple(names[field] for field in sorted(names))
        if identity in seen:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Review binding manifest contains duplicate bindings."
            )
        seen.add(identity)
        review_path = (path.parent / names["review"]).resolve()
        summary_path = (path.parent / names["compatibility_summary"]).resolve()
        manifest_path = (path.parent / names["compatibility_manifest"]).resolve()
        artifact_directory = (path.parent / names["artifact_directory"]).resolve()
        try:
            review = json.loads(review_path.read_text(encoding="utf-8"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Review binding JSON evidence is unreadable."
            ) from exc
        suffix = summary_path.suffix.lower()
        if suffix == ".csv":
            summary = pd.read_csv(summary_path)
        elif suffix in {".parquet", ".pq"}:
            summary = pd.read_parquet(summary_path)
        else:
            raise IntervalPolicyRetainedCompatibilityReviewLedgerError(
                "Compatibility summaries must be CSV or Parquet."
            )
        loaded.append((review, summary, manifest, artifact_directory))
    return loaded


__all__ = [
    "LEDGER_CONTRACT_VERSION",
    "LEDGER_SAFETY_FIELDS",
    "IntervalPolicyRetainedCompatibilityReviewLedgerError",
    "build_retained_compatibility_review_ledger",
    "read_review_bindings",
    "render_retained_compatibility_review_ledger",
    "verify_retained_compatibility_review_ledger",
    "write_retained_compatibility_review_ledger",
]
