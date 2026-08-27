from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting._interval_policy_revision_sensitivity_common import (
    REVISION_SENSITIVITY_SAFETY_FIELDS,
    REVISION_SENSITIVITY_CONTRACT_VERSION,
    digest,
)
from forecasting.interval_policy_revision_disposition import (
    DISPOSITION_SAFETY_FIELDS,
    IntervalPolicyRevisionDispositionError,
    create_revision_sensitivity_disposition,
    verify_revision_sensitivity_disposition,
    write_revision_sensitivity_disposition,
)
from forecasting.run_interval_policy_revision_disposition import main


ROOT = Path(__file__).resolve().parents[1]


def summary() -> pd.DataFrame:
    rows = []
    statuses = ("healthy", "warning", "failed")
    revised_status = {
        "healthy": "healthy",
        "warning": "healthy",
        "failed": "warning",
    }
    for scenario in statuses:
        for candidate_id in ("active-reference", "revised-candidate-r2"):
            active = candidate_id == "active-reference"
            candidate_status = scenario if active else revised_status[scenario]
            changed = candidate_status != scenario
            row = {
                "sensitivity_run_id": "ips-" + "3" * 24,
                "sensitivity_run_timestamp_utc": "2026-01-20T04:00:00Z",
                "trend_run_id": "iht-" + "2" * 24,
                "scenario": scenario,
                "candidate_id": candidate_id,
                "candidate_role": (
                    "active_reference" if active else "review_candidate"
                ),
                "candidate_version": (
                    "prediction-interval-monitoring-policy-v1"
                    if active
                    else "interval-monitoring-review-candidate-v3"
                ),
                "retained_monitor_status": scenario,
                "active_reference_status": scenario,
                "candidate_status": candidate_status,
                "status_changed_from_active": changed,
                "sensitivity_classification": (
                    "active_reference"
                    if active
                    else "status_sensitive"
                    if changed
                    else "status_robust"
                ),
                "slice_count": 96,
                "changed_slice_count": 0 if active else 48 if changed else 0,
                "human_review_required": (not active and changed),
                "source_revision_review_id": "iprr-" + "4" * 24,
                "source_revision_review_sha256": "4" * 64,
                "source_revision_package_id": "ipr-" + "5" * 24,
                "source_revision_package_sha256": "5" * 64,
                "source_decision_id": "ipd-" + "6" * 24,
                "source_decision_sha256": "6" * 64,
                "revised_candidate_id": "revised-candidate-r2",
                "revised_candidate_version": (
                    "interval-monitoring-review-candidate-v3"
                ),
                "revised_candidate_sha256": "7" * 64,
                "revision_sensitivity_contract_version": (
                    REVISION_SENSITIVITY_CONTRACT_VERSION
                ),
            }
            row.update(
                {field: False for field in REVISION_SENSITIVITY_SAFETY_FIELDS}
            )
            rows.append(row)
    return pd.DataFrame(rows)


def manifest() -> dict:
    document = {
        "sensitivity_run_id": "ips-" + "3" * 24,
        "revision_sensitivity_contract_version": (
            REVISION_SENSITIVITY_CONTRACT_VERSION
        ),
        "source_revision_review_id": "iprr-" + "4" * 24,
        "source_revision_review_sha256": "4" * 64,
        "source_revision_package_id": "ipr-" + "5" * 24,
        "source_revision_package_sha256": "5" * 64,
        "revised_candidate_id": "revised-candidate-r2",
        "revised_candidate_sha256": "7" * 64,
        "artifacts": [
            {"role": "slices", "path": "slices.csv", "sha256": "a" * 64, "row_count": 6},
            {"role": "summary", "path": "summary.csv", "sha256": "b" * 64, "row_count": 6},
            {"role": "report", "path": "report.md", "sha256": "c" * 64, "row_count": None},
        ],
        **{field: False for field in REVISION_SENSITIVITY_SAFETY_FIELDS},
    }
    document["manifest_sha256"] = digest(document)
    return document


def create(disposition: str, requested_actions=()):
    return create_revision_sensitivity_disposition(
        summary(),
        manifest(),
        disposition=disposition,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-132",
        rationale=(
            "The retained reviewed-candidate sensitivity evidence was assessed "
            "without authorising a policy change."
        ),
        requested_actions=requested_actions,
        disposed_at_utc="2026-01-20T05:00:00Z",
    )


def test_suitable_disposition_is_named_hashed_and_non_activating():
    document = create("suitable_for_separate_implementation_proposal")
    verify_revision_sensitivity_disposition(
        document, summary(), manifest()
    )
    assert document["target_candidate_id"] == "revised-candidate-r2"
    assert document["disposition_effect"] == (
        "separate_implementation_proposal_review_required"
    )
    assert document["follow_up_human_action_required"] is True
    assert all(document[field] is False for field in DISPOSITION_SAFETY_FIELDS)


def test_retain_and_revision_semantics_are_distinct():
    retained = create("retain_active_policy")
    assert retained["target_candidate_id"] == "active-reference"
    assert retained["follow_up_human_action_required"] is False

    with pytest.raises(
        IntervalPolicyRevisionDispositionError, match="at least one"
    ):
        create("request_another_revision")
    revised = create(
        "request_another_revision",
        ["Reduce the permitted coverage shortfall before another review."],
    )
    assert revised["requested_actions"]
    with pytest.raises(
        IntervalPolicyRevisionDispositionError,
        match="Only request_another_revision",
    ):
        create(
            "reject_revised_candidate",
            ["This action must not be retained on a rejection."],
        )


def test_tampered_source_evidence_is_rejected():
    document = create("reject_revised_candidate")
    changed = summary()
    changed.loc[
        (changed["scenario"] == "warning")
        & (changed["candidate_id"] == "revised-candidate-r2"),
        "changed_slice_count",
    ] = 47
    with pytest.raises(
        IntervalPolicyRevisionDispositionError,
        match="does not match the retained summary",
    ):
        verify_revision_sensitivity_disposition(
            document, changed, manifest()
        )

    bad_manifest = manifest()
    bad_manifest["revised_candidate_id"] = "other"
    with pytest.raises(
        IntervalPolicyRevisionDispositionError, match="hash is invalid"
    ):
        verify_revision_sensitivity_disposition(
            document, summary(), bad_manifest
        )


def test_disposition_satisfies_versioned_schema():
    document = create("reject_revised_candidate")
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_revision_disposition_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(document)
    )
    assert errors == []


def test_immutable_writer_and_cli(tmp_path):
    summary_path = tmp_path / "summary.csv"
    manifest_path = tmp_path / "manifest.json"
    output_path = tmp_path / "output"
    summary().to_csv(summary_path, index=False)
    manifest_path.write_text(json.dumps(manifest()), encoding="utf-8")
    args = [
        "--revision-sensitivity-summary",
        str(summary_path),
        "--revision-sensitivity-manifest",
        str(manifest_path),
        "--disposition",
        "reject_revised_candidate",
        "--reviewer-name",
        "Alex Reviewer",
        "--reviewer-role",
        "Data Platform Owner",
        "--review-ticket",
        "GOV-132",
        "--rationale",
        "The revised candidate was rejected after reviewing retained evidence.",
        "--disposed-at-utc",
        "2026-01-20T05:00:00Z",
        "--output-dir",
        str(output_path),
    ]
    assert main(args) == 0
    assert len(list(output_path.glob("*.json"))) == 1
    assert len(list(output_path.glob("*.md"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)

    direct = create("retain_active_policy")
    direct_dir = tmp_path / "direct"
    write_revision_sensitivity_disposition(
        direct_dir, direct, summary(), manifest()
    )
    assert len(list(direct_dir.iterdir())) == 2
