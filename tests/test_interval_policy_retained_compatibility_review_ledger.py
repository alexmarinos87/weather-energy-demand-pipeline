from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

import forecasting.interval_policy_retained_compatibility_review_ledger as module
from forecasting.interval_policy_retained_compatibility_review import (
    DECISION_EFFECTS,
    REVIEW_CONTRACT_VERSION,
    REVIEW_SAFETY_FIELDS,
)
from forecasting.interval_policy_retained_compatibility_review_ledger import (
    LEDGER_SAFETY_FIELDS,
    IntervalPolicyRetainedCompatibilityReviewLedgerError,
    build_retained_compatibility_review_ledger,
    verify_retained_compatibility_review_ledger,
    write_retained_compatibility_review_ledger,
)
from forecasting.run_interval_policy_retained_compatibility_review_ledger import (
    main,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def source_review_verification(monkeypatch):
    monkeypatch.setattr(
        module,
        "verify_retained_compatibility_review",
        lambda *args, **kwargs: None,
    )


def review(
    index: int,
    *,
    decision: str = "accept_non_retroactive_transition",
    compatibility_run_id: str | None = None,
    review_id: str | None = None,
) -> dict:
    requires_follow_up = decision != "accept_non_retroactive_transition"
    return {
        "review_id": review_id or f"ipcr-{index:024x}",
        "review_revision": 1,
        "compatibility_run_id": compatibility_run_id or f"ipca-{index:024x}",
        "compatibility_run_timestamp_utc": f"2026-01-{index:02d}T00:00:00Z",
        "trend_run_id": f"iht-{index:024x}",
        "compatibility_summary_sha256": f"{index:x}" * 64,
        "compatibility_manifest_sha256": f"{index + 1:x}" * 64,
        "previous_policy_id": "previous-five-point",
        "previous_shortfall_threshold_pct_points": 5.0,
        "current_policy_id": "reviewed-three-point",
        "current_shortfall_threshold_pct_points": 3.0,
        "decision": decision,
        "decision_effect": DECISION_EFFECTS[decision],
        "reviewer_name": f"Reviewer {index}",
        "reviewer_role": "Data Platform Owner",
        "review_ticket": f"GOV-{140 + index}",
        "rationale": "A sufficiently detailed retained compatibility review rationale.",
        "requested_actions": (
            ["Prepare a separate reviewed follow-up evidence package."]
            if requires_follow_up
            else []
        ),
        "reviewed_at_utc": f"2026-01-{index:02d}T01:00:00Z",
        "scenario_evidence": [
            {
                "scenario": "stable",
                "retained_monitor_status": "healthy",
                "previous_policy_status": "healthy",
                "current_policy_status": "healthy",
                "compatibility_classification": "fully_compatible",
                "retained_status_compatibility": "matches_both_policies",
                "changed_slice_count": 0,
                "newly_failed_slice_count": 0,
                "human_review_required": False,
            }
        ],
        "named_human_review_confirmed": True,
        "follow_up_human_action_required": requires_follow_up,
        **{field: False for field in REVIEW_SAFETY_FIELDS},
        "review_contract_version": REVIEW_CONTRACT_VERSION,
        "review_sha256": f"{index + 2:x}" * 64,
    }


def binding(document: dict, tmp_path: Path):
    return document, pd.DataFrame({"placeholder": [1]}), {}, tmp_path


def build(tmp_path: Path):
    bindings = [
        binding(review(2, decision="request_historical_annotation"), tmp_path),
        binding(review(1), tmp_path),
    ]
    return build_retained_compatibility_review_ledger(
        bindings,
        ledger_run_id="ipcrl-" + "a" * 24,
        ledger_run_timestamp_utc="2026-01-03T00:00:00Z",
    )


def json_row(frame: pd.DataFrame) -> dict:
    row = frame.iloc[0].to_dict()
    for key, value in list(row.items()):
        if isinstance(value, pd.Timestamp):
            row[key] = value.isoformat()
        elif pd.isna(value):
            row[key] = None
    return row


def test_ledger_orders_verified_reviews_and_retains_no_action_authority(tmp_path):
    entries, summary = build(tmp_path)
    verify_retained_compatibility_review_ledger(entries, summary)
    assert entries["review_sequence"].tolist() == [1, 2]
    assert entries["review_id"].tolist() == [
        "ipcr-" + f"{1:024x}",
        "ipcr-" + f"{2:024x}",
    ]
    assert summary.loc[0, "review_count"] == 2
    assert summary.loc[0, "accepted_transition_count"] == 1
    assert summary.loc[0, "historical_annotation_request_count"] == 1
    assert summary.loc[0, "follow_up_review_count"] == 1
    assert summary.loc[0, "human_review_required"]
    assert all(
        not bool(summary.loc[0, field]) for field in LEDGER_SAFETY_FIELDS
    )


def test_duplicate_review_ids_and_conflicting_run_decisions_are_rejected(tmp_path):
    same_id = "ipcr-" + "b" * 24
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewLedgerError,
        match="Duplicate",
    ):
        build_retained_compatibility_review_ledger(
            [
                binding(review(1, review_id=same_id), tmp_path),
                binding(review(2, review_id=same_id), tmp_path),
            ],
            ledger_run_timestamp_utc="2026-01-03T00:00:00Z",
        )
    same_run = "ipca-" + "c" * 24
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewLedgerError,
        match="Conflicting",
    ):
        build_retained_compatibility_review_ledger(
            [
                binding(review(1, compatibility_run_id=same_run), tmp_path),
                binding(
                    review(
                        2,
                        decision="request_compatibility_reassessment",
                        compatibility_run_id=same_run,
                    ),
                    tmp_path,
                ),
            ],
            ledger_run_timestamp_utc="2026-01-03T00:00:00Z",
        )


def test_ledger_rejects_tampered_counts_hash_and_authority(tmp_path):
    entries, summary = build(tmp_path)
    changed = summary.copy()
    changed.loc[0, "review_count"] = 99
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewLedgerError,
        match="review_count",
    ):
        verify_retained_compatibility_review_ledger(entries, changed)
    changed = summary.copy()
    changed.loc[0, "ledger_sha256"] = "0" * 64
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewLedgerError,
        match="SHA-256",
    ):
        verify_retained_compatibility_review_ledger(entries, changed)
    changed = summary.copy()
    changed.loc[0, "historical_annotation_applied"] = True
    with pytest.raises(
        IntervalPolicyRetainedCompatibilityReviewLedgerError,
        match="must be false",
    ):
        verify_retained_compatibility_review_ledger(entries, changed)


def test_ledger_summary_satisfies_versioned_schema(tmp_path):
    _, summary = build(tmp_path)
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "interval_policy_retained_compatibility_review_ledger_summary_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(json_row(summary))
    )
    assert errors == []


def test_writer_and_cli_are_immutable(tmp_path):
    entries, summary = build(tmp_path)
    direct = tmp_path / "direct"
    outputs = write_retained_compatibility_review_ledger(
        direct,
        entries,
        summary,
        output_format="csv",
    )
    assert set(outputs) == {"entries", "summary", "report", "manifest"}
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_retained_compatibility_review_ledger(
            direct,
            entries,
            summary,
            output_format="csv",
        )

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    artifacts = inputs / "artifacts"
    artifacts.mkdir()
    document = review(1)
    (inputs / "review.json").write_text(json.dumps(document), encoding="utf-8")
    pd.DataFrame({"placeholder": [1]}).to_csv(
        inputs / "summary.csv", index=False
    )
    (inputs / "manifest.json").write_text("{}", encoding="utf-8")
    binding_manifest = {
        "bindings": [
            {
                "review": "review.json",
                "compatibility_summary": "summary.csv",
                "compatibility_manifest": "manifest.json",
                "artifact_directory": "artifacts",
            }
        ]
    }
    (inputs / "bindings.json").write_text(
        json.dumps(binding_manifest), encoding="utf-8"
    )
    output = tmp_path / "cli"
    args = [
        "--binding-manifest",
        str(inputs / "bindings.json"),
        "--ledger-run-id",
        "ipcrl-" + "d" * 24,
        "--ledger-run-timestamp-utc",
        "2026-01-03T00:00:00Z",
        "--output-dir",
        str(output),
        "--output-format",
        "csv",
    ]
    assert main(args) == 0
    assert len(list(output.iterdir())) == 4
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)
