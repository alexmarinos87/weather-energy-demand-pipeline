from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_retained_compatibility import (
    evaluate_retained_policy_compatibility,
)
from forecasting.interval_policy_retained_compatibility_manifest import (
    build_compatibility_manifest,
)
from forecasting.interval_policy_retained_compatibility_review import (
    REVIEW_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityReviewError,
    create_retained_compatibility_review,
    verify_retained_compatibility_review,
    write_retained_compatibility_review,
)
from forecasting.run_interval_policy_retained_compatibility_review import main


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "ipca-" + "1" * 24
REVIEW_ID = "ipcr-" + "2" * 24
RUN_TIMESTAMP = "2026-01-20T01:00:00Z"
REVIEW_TIMESTAMP = "2026-01-20T02:00:00Z"


def slice_trends() -> pd.DataFrame:
    rows = []
    for index, (scenario, retained, shortfall) in enumerate(
        (
            ("stable", "healthy", 1.0),
            ("tightened", "healthy", 4.0),
            ("failed", "failed", 10.0),
        ),
        start=1,
    ):
        rows.append(
            {
                "trend_run_id": "iht-" + "3" * 24,
                "trend_run_timestamp_utc": "2026-01-20T00:00:00Z",
                "scenario": scenario,
                "source_area": "east_midlands",
                "resource_id": "resource-1",
                "city": "Nottingham,GB",
                "requested_horizon_minutes": 30,
                "model_name": "ridge_weather_lag",
                "feature_contract_version": "time-horizon-v1",
                "target_coverage_level": 0.90,
                "interval_contract_version": "prediction-interval-v1",
                "monitor_status": retained,
                "trend_contract_version": "interval-health-trend-v1",
                "recent_interval_run_count": 3,
                "reference_interval_run_count": 6,
                "reference_history_sufficient": True,
                "latest_interval_run_timestamp_utc": (
                    f"2026-01-19T{20 + index:02d}:00:00Z"
                ),
                "latest_evaluation_end_utc": (
                    f"2026-01-19T{19 + index:02d}:00:00Z"
                ),
                "recent_minimum_calibration_observation_count": 30,
                "latest_coverage_shortfall_pct_points": shortfall,
                "coverage_drop_pct_points": 0.0,
                "average_interval_width_increase_pct": 0.0,
                "calibration_history_drop_pct": 0.0,
            }
        )
    return pd.DataFrame(rows)


def source_evidence(tmp_path: Path):
    slices, summary, report = evaluate_retained_policy_compatibility(
        slice_trends(),
        compatibility_run_id=RUN_ID,
        compatibility_run_timestamp=RUN_TIMESTAMP,
    )
    artifact_directory = tmp_path / "compatibility"
    artifact_directory.mkdir()
    paths = {
        "slices": artifact_directory / "slices.csv",
        "summary": artifact_directory / "summary.csv",
        "report": artifact_directory / "report.md",
    }
    slices.to_csv(paths["slices"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(report, encoding="utf-8")
    manifest = build_compatibility_manifest(summary, artifacts=paths)
    return summary, manifest, artifact_directory, paths


def create_review(tmp_path: Path, **overrides):
    summary, manifest, artifact_directory, paths = source_evidence(tmp_path)
    values = {
        "artifact_directory": artifact_directory,
        "decision": "accept_non_retroactive_transition",
        "reviewer_name": "Alex Reviewer",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": "GOV-140",
        "rationale": (
            "The retained comparison supports a non-retroactive transition while "
            "preserving all historical monitoring evidence."
        ),
        "reviewed_at_utc": REVIEW_TIMESTAMP,
        "review_id": REVIEW_ID,
    }
    values.update(overrides)
    review = create_retained_compatibility_review(
        summary,
        manifest,
        **values,
    )
    return review, summary, manifest, artifact_directory, paths


def test_acceptance_is_named_hashed_and_non_retroactive(tmp_path):
    review, summary, manifest, artifact_directory, _ = create_review(tmp_path)
    verify_retained_compatibility_review(
        review,
        summary,
        manifest,
        artifact_directory=artifact_directory,
    )
    assert review["decision"] == "accept_non_retroactive_transition"
    assert review["decision_effect"] == "non_retroactive_transition_accepted"
    assert review["named_human_review_confirmed"] is True
    assert review["follow_up_human_action_required"] is False
    assert review["requested_actions"] == []
    tightened = next(
        item for item in review["scenario_evidence"]
        if item["scenario"] == "tightened"
    )
    assert tightened["retained_monitor_status"] == "healthy"
    assert tightened["previous_policy_status"] == "healthy"
    assert tightened["current_policy_status"] == "failed"
    assert tightened["newly_failed_slice_count"] == 1
    assert all(review[field] is False for field in REVIEW_SAFETY_FIELDS)


def test_follow_up_decisions_require_actions_and_acceptance_rejects_them(tmp_path):
    summary, manifest, artifact_directory, _ = source_evidence(tmp_path)
    common = {
        "artifact_directory": artifact_directory,
        "reviewer_name": "Alex Reviewer",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": "GOV-140",
        "rationale": (
            "The compatibility evidence requires a separate bounded follow-up "
            "without modifying historical monitoring rows."
        ),
        "reviewed_at_utc": REVIEW_TIMESTAMP,
    }
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewError,
        match="at least one",
    ):
        create_retained_compatibility_review(
            summary,
            manifest,
            decision="request_historical_annotation",
            **common,
        )
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewError,
        match="cannot contain requested_actions",
    ):
        create_retained_compatibility_review(
            summary,
            manifest,
            decision="accept_non_retroactive_transition",
            requested_actions=[
                "Prepare a separate annotation proposal for historical rows."
            ],
            **common,
        )
    review = create_retained_compatibility_review(
        summary,
        manifest,
        decision="request_compatibility_reassessment",
        requested_actions=[
            "Repeat the comparison when one more complete trend run is retained."
        ],
        **common,
    )
    assert review["follow_up_human_action_required"] is True
    assert review["decision_effect"] == "new_compatibility_assessment_required"


def test_review_rejects_early_timestamp_source_tampering_and_self_tampering(tmp_path):
    summary, manifest, artifact_directory, _ = source_evidence(tmp_path)
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewError,
        match="cannot precede",
    ):
        create_retained_compatibility_review(
            summary,
            manifest,
            artifact_directory=artifact_directory,
            decision="accept_non_retroactive_transition",
            reviewer_name="Alex Reviewer",
            reviewer_role="Data Platform Owner",
            review_ticket="GOV-140",
            rationale="A sufficiently detailed review rationale for rejection.",
            reviewed_at_utc="2026-01-19T23:00:00Z",
        )
    review = create_retained_compatibility_review(
        summary,
        manifest,
        artifact_directory=artifact_directory,
        decision="accept_non_retroactive_transition",
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-140",
        rationale="A sufficiently detailed review rationale for verification.",
        reviewed_at_utc=REVIEW_TIMESTAMP,
    )
    changed_summary = summary.copy()
    changed_summary.loc[0, "newly_failed_slice_count"] = 99
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewError,
        match="summary digest",
    ):
        verify_retained_compatibility_review(
            review,
            changed_summary,
            manifest,
            artifact_directory=artifact_directory,
        )
    review["rationale"] += " changed"
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewError,
        match="review hash",
    ):
        verify_retained_compatibility_review(
            review,
            summary,
            manifest,
            artifact_directory=artifact_directory,
        )


def test_review_satisfies_versioned_schema(tmp_path):
    review, _, _, _, _ = create_review(tmp_path)
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_retained_compatibility_review_schema.json"
        ).read_text(encoding="utf-8")
    )
    assert list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(review)
    ) == []


def test_writer_and_cli_are_immutable(tmp_path):
    review, summary, manifest, artifact_directory, paths = create_review(tmp_path)
    direct = tmp_path / "direct"
    outputs = write_retained_compatibility_review(
        direct,
        review,
        summary,
        manifest,
        artifact_directory=artifact_directory,
    )
    assert set(outputs) == {"json", "markdown"}
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_retained_compatibility_review(
            direct,
            review,
            summary,
            manifest,
            artifact_directory=artifact_directory,
        )

    manifest_path = artifact_directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    output = tmp_path / "cli"
    args = [
        "--compatibility-summary",
        str(paths["summary"]),
        "--compatibility-manifest",
        str(manifest_path),
        "--artifact-directory",
        str(artifact_directory),
        "--decision",
        "accept_non_retroactive_transition",
        "--reviewer-name",
        "Alex Reviewer",
        "--reviewer-role",
        "Data Platform Owner",
        "--review-ticket",
        "GOV-140",
        "--rationale",
        "Accept the non-retroactive transition while retaining historical evidence.",
        "--reviewed-at-utc",
        REVIEW_TIMESTAMP,
        "--review-id",
        REVIEW_ID,
        "--output-dir",
        str(output),
    ]
    assert main(args) == 0
    assert len(list(output.glob("*.json"))) == 1
    assert len(list(output.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)
