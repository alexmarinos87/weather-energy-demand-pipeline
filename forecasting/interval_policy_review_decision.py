from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from forecasting._interval_policy_review_summary import (
    ALLOWED_DECISIONS,
    AUTHORITY_FIELDS,
    DECISION_CONTRACT_VERSION,
    DECISION_EFFECTS,
    DECISION_ID_PATTERN,
    DECISION_SAFETY_FIELDS,
    IntervalPolicyReviewDecisionError,
    _canonical,
    prepare_sensitivity_summary,
    sensitivity_summary_sha256,
)
from forecasting._interval_policy_review_create import create_policy_review_decision
from forecasting._interval_policy_review_verify import verify_policy_review_decision


def render_policy_review_decision(decision: dict[str, Any]) -> str:
    """Render the immutable decision as human-readable Markdown."""
    lines = [
        "# Interval-monitoring policy review decision",
        "",
        f"- Decision ID: `{decision['decision_id']}`",
        f"- Sensitivity run: `{decision['sensitivity_run_id']}`",
        f"- Decision: `{decision['decision']}`",
        f"- Target candidate: `{decision['target_candidate_id']}`",
        f"- Reviewer: {decision['reviewer_name']} ({decision['reviewer_role']})",
        f"- Review ticket: `{decision['review_ticket']}`",
        f"- Decision time: `{decision['decision_timestamp_utc']}`",
        "",
        "## Rationale",
        "",
        decision["rationale"],
        "",
    ]
    if decision["requested_changes"]:
        lines.extend(["## Requested changes", ""])
        lines.extend(f"- {item}" for item in decision["requested_changes"])
        lines.append("")
    lines.extend(
        [
            "## Retained scenario evidence",
            "",
            "| Scenario | Retained | Active reference | Target candidate | Classification | Changed slices |",
            "| --- | --- | --- | --- | --- | ---: |",
        ]
    )
    for row in decision["scenario_evidence"]:
        lines.append(
            f"| {row['scenario']} | {row['retained_monitor_status']} | "
            f"{row['active_reference_status']} | {row['target_candidate_status']} | "
            f"{row['sensitivity_classification']} | {row['changed_slice_count']} |"
        )
    lines.extend(
        [
            "",
            "This receipt records human review evidence only. It does not activate candidate thresholds or update the active monitoring policy.",
            "No interval recalibration, model change, schedule change, promotion, alert delivery, deployment, or external publication is performed.",
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
    raise IntervalPolicyReviewDecisionError(
        "Sensitivity summary input must be CSV or Parquet."
    )


def write_policy_review_decision(
    output_directory: Path,
    decision: dict[str, Any],
    sensitivity_summary: pd.DataFrame,
) -> tuple[Path, Path]:
    """Write immutable JSON and Markdown decision evidence."""
    verify_policy_review_decision(decision, sensitivity_summary)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / (
        f"interval_policy_review_decision_{decision['decision_id']}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_review_decision_{decision['decision_id']}.md"
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
            json.dumps(_canonical(decision), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_paths[1].write_text(
            render_policy_review_decision(decision), encoding="utf-8"
        )
        temporary_paths[0].replace(json_path)
        temporary_paths[1].replace(markdown_path)
    finally:
        for temporary in temporary_paths:
            temporary.unlink(missing_ok=True)
    return json_path, markdown_path
