from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.interval_policy_decision_ledger import (
    LEDGER_SAFETY_FIELDS,
    IntervalPolicyDecisionLedgerError,
    build_policy_decision_ledger,
    ledger_digest,
    load_decision_bindings,
    write_policy_decision_ledger,
)
from forecasting.interval_policy_review_decision import (
    create_policy_review_decision,
    write_policy_review_decision,
)
from forecasting.run_interval_policy_decision_ledger import main

ROOT = Path(__file__).resolve().parents[1]
LEDGER_RUN_ID = "ipl-" + "3" * 24


def summary(digit: str) -> pd.DataFrame:
    retained = {"healthy": "healthy", "warning": "warning", "failed": "failed"}
    candidates = {
        "active-reference": retained,
        "stricter-review": {
            "healthy": "healthy",
            "warning": "failed",
            "failed": "failed",
        },
        "tolerant-review": {
            "healthy": "healthy",
            "warning": "healthy",
            "failed": "healthy",
        },
    }
    rows = []
    for scenario, retained_status in retained.items():
        for candidate_id, statuses in candidates.items():
            role = (
                "active_reference"
                if candidate_id == "active-reference"
                else "review_candidate"
            )
            candidate_status = statuses[scenario]
            changed = candidate_status != retained_status
            rows.append(
                {
                    "sensitivity_run_id": "ips-" + digit * 24,
                    "sensitivity_run_timestamp_utc": "2026-01-20T00:00:00Z",
                    "trend_run_id": "iht-" + digit * 24,
                    "scenario": scenario,
                    "candidate_id": candidate_id,
                    "candidate_role": role,
                    "candidate_version": "interval-monitoring-review-candidate-v1",
                    "retained_monitor_status": retained_status,
                    "active_reference_status": retained_status,
                    "candidate_status": candidate_status,
                    "status_changed_from_active": changed,
                    "sensitivity_classification": (
                        "active_reference"
                        if role == "active_reference"
                        else "status_sensitive"
                        if changed
                        else "status_robust"
                    ),
                    "slice_count": 96,
                    "changed_slice_count": 48 if changed else 0,
                    "human_review_required": role == "review_candidate" and changed,
                    "sensitivity_contract_version": "interval-policy-sensitivity-v1",
                    "active_policy_updated": False,
                    "candidate_thresholds_activated": False,
                    "retained_evidence_mutated": False,
                    "interval_recalibration_performed": False,
                    "model_change_performed": False,
                    "schedule_change_performed": False,
                    "promotion_change_performed": False,
                    "alert_delivery_performed": False,
                }
            )
    return pd.DataFrame(rows)


def decision(frame, outcome, target, timestamp, changes=()):
    return create_policy_review_decision(
        frame,
        decision=outcome,
        target_candidate_id=target,
        reviewer_name="Alex Reviewer",
        reviewer_role="Data Platform Owner",
        review_ticket="GOV-128",
        rationale=(
            "The retained counterfactual evidence was reviewed for ledger inclusion."
        ),
        requested_changes=changes,
        decision_timestamp_utc=timestamp,
    )


def bindings():
    first, second = summary("1"), summary("2")
    return [
        (
            decision(
                first,
                "retain_active_policy",
                "active-reference",
                "2026-01-20T01:00:00Z",
            ),
            first,
        ),
        (
            decision(
                first,
                "reject_candidate",
                "stricter-review",
                "2026-01-20T01:05:00Z",
            ),
            first,
        ),
        (
            decision(
                second,
                "request_candidate_revision",
                "tolerant-review",
                "2026-01-20T01:10:00Z",
                [
                    "Reduce the permitted coverage shortfall and document the operational rationale."
                ],
            ),
            second,
        ),
    ]


def json_row(row):
    result = {}
    for key, value in row.items():
        result[key] = (
            value.isoformat()
            if isinstance(value, pd.Timestamp)
            else value.item()
            if hasattr(value, "item")
            else value
        )
    return result


def test_ledger_orders_verified_decisions_and_keeps_authority_false():
    entries, overview = build_policy_decision_ledger(
        reversed(bindings()),
        ledger_run_id=LEDGER_RUN_ID,
        ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
    )
    assert entries["decision_sequence"].tolist() == [1, 2, 3]
    assert entries["decision"].tolist() == [
        "retain_active_policy",
        "reject_candidate",
        "request_candidate_revision",
    ]
    assert overview.loc[0, "decision_count"] == 3
    assert overview.loc[0, "sensitivity_run_count"] == 2
    assert overview.loc[0, "conflict_count"] == 0
    for field in LEDGER_SAFETY_FIELDS:
        assert set(entries[field]) == {False}
        assert not bool(overview.loc[0, field])


def test_duplicate_and_conflicting_decisions_fail_closed():
    pairs = bindings()
    with pytest.raises(
        IntervalPolicyDecisionLedgerError,
        match="duplicate decision IDs",
    ):
        build_policy_decision_ledger(
            [pairs[0], pairs[0]],
            ledger_run_id=LEDGER_RUN_ID,
            ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
        )
    frame = summary("1")
    first = decision(
        frame,
        "reject_candidate",
        "stricter-review",
        "2026-01-20T01:05:00Z",
    )
    second = decision(
        frame,
        "request_candidate_revision",
        "stricter-review",
        "2026-01-20T01:06:00Z",
        [
            "Provide a bounded revised candidate with a narrower freshness tolerance."
        ],
    )
    with pytest.raises(
        IntervalPolicyDecisionLedgerError,
        match="conflicting decisions",
    ):
        build_policy_decision_ledger(
            [(first, frame), (second, frame)],
            ledger_run_id=LEDGER_RUN_ID,
            ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
        )


def test_tampered_source_evidence_is_rejected():
    item, frame = bindings()[1]
    changed = frame.copy()
    changed.loc[
        (changed["scenario"] == "warning")
        & (changed["candidate_id"] == "stricter-review"),
        "changed_slice_count",
    ] = 47
    with pytest.raises(
        IntervalPolicyDecisionLedgerError,
        match="failed verification",
    ):
        build_policy_decision_ledger(
            [(item, changed)],
            ledger_run_id=LEDGER_RUN_ID,
            ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
        )


def test_decision_cannot_postdate_ledger_run():
    with pytest.raises(
        IntervalPolicyDecisionLedgerError,
        match="after its ledger",
    ):
        build_policy_decision_ledger(
            [bindings()[0]],
            ledger_run_id=LEDGER_RUN_ID,
            ledger_run_timestamp_utc="2026-01-20T00:30:00Z",
        )


def test_ledger_rows_satisfy_versioned_schemas():
    entries, overview = build_policy_decision_ledger(
        bindings(),
        ledger_run_id=LEDGER_RUN_ID,
        ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
    )
    for filename, row in [
        (
            "interval_policy_decision_ledger_entry_schema.json",
            json_row(entries.iloc[0]),
        ),
        (
            "interval_policy_decision_ledger_summary_schema.json",
            json_row(overview.iloc[0]),
        ),
    ]:
        schema = json.loads(
            (ROOT / "data-contracts" / filename).read_text()
        )
        assert list(
            Draft202012Validator(
                schema,
                format_checker=FormatChecker(),
            ).iter_errors(row)
        ) == []


def test_digest_detects_retained_entry_changes():
    entries, overview = build_policy_decision_ledger(
        bindings(),
        ledger_run_id=LEDGER_RUN_ID,
        ledger_run_timestamp_utc="2026-01-20T02:00:00Z",
    )
    original = ledger_digest(entries, overview)
    entries.loc[0, "rationale"] = "A materially different retained rationale."
    assert ledger_digest(entries, overview) != original


def test_manifest_cli_writes_immutable_outputs(tmp_path):
    manifest_rows = []
    for index, (item, frame) in enumerate(bindings()[:2], start=1):
        decision_dir = tmp_path / f"decision-{index}"
        decision_dir.mkdir()
        json_path, _ = write_policy_review_decision(
            decision_dir,
            item,
            frame,
        )
        summary_path = tmp_path / f"summary-{index}.csv"
        frame.to_csv(summary_path, index=False)
        manifest_rows.append(
            {
                "decision": str(json_path.relative_to(tmp_path)),
                "sensitivity_summary": summary_path.name,
            }
        )
    manifest = tmp_path / "bindings.json"
    manifest.write_text(json.dumps({"bindings": manifest_rows}))
    assert len(load_decision_bindings(manifest)) == 2
    output = tmp_path / "ledger"
    args = [
        "--bindings",
        str(manifest),
        "--output-dir",
        str(output),
        "--output-format",
        "csv",
        "--ledger-run-id",
        LEDGER_RUN_ID,
        "--ledger-run-timestamp-utc",
        "2026-01-20T02:00:00Z",
    ]
    assert main(args) == 0
    assert len(list(output.glob("*.csv"))) == 2
    assert len(list(output.glob("*.md"))) == 1
    assert len(list(output.glob("*.json"))) == 1
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        main(args)
