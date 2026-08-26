from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Iterable

import pandas as pd

from forecasting._interval_policy_decision_ledger_common import (
    ENTRY_COLUMNS,
    LEDGER_CONTRACT_VERSION,
    LEDGER_RUN_ID_PATTERN,
    LEDGER_SAFETY_FIELDS,
    SUMMARY_COLUMNS,
    IntervalPolicyDecisionLedgerError,
    _decision_row,
    _digest,
    _utc,
)
from forecasting.interval_policy_review_decision import (
    IntervalPolicyReviewDecisionError,
    verify_policy_review_decision,
)


def build_policy_decision_ledger(
    bindings: Iterable[tuple[dict[str, Any], pd.DataFrame]],
    *,
    ledger_run_id: str | None = None,
    ledger_run_timestamp_utc: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Verify G27 decisions and create one immutable conflict-free ledger."""
    run_id = ledger_run_id or "ipl-" + hashlib.sha256(
        str(datetime.now(timezone.utc).timestamp()).encode("utf-8")
    ).hexdigest()[:24]
    if not LEDGER_RUN_ID_PATTERN.fullmatch(run_id):
        raise IntervalPolicyDecisionLedgerError(
            "ledger_run_id must match ipl- plus 24 lowercase hexadecimal characters."
        )
    run_timestamp = _utc(
        ledger_run_timestamp_utc or datetime.now(timezone.utc),
        "ledger_run_timestamp_utc",
    )
    rows: list[dict[str, Any]] = []
    for decision, summary in bindings:
        try:
            verify_policy_review_decision(decision, summary)
        except IntervalPolicyReviewDecisionError as exc:
            raise IntervalPolicyDecisionLedgerError(
                f"Decision evidence failed verification: {exc}"
            ) from exc
        rows.append(
            _decision_row(
                decision,
                ledger_run_id=run_id,
                ledger_run_timestamp=run_timestamp,
            )
        )
    if not rows:
        raise IntervalPolicyDecisionLedgerError(
            "At least one verified decision binding is required."
        )
    entries = pd.DataFrame(rows).sort_values(
        ["decision_timestamp_utc", "decision_id"]
    ).reset_index(drop=True)
    entries["decision_sequence"] = range(1, len(entries) + 1)
    if entries["decision_id"].duplicated().any():
        raise IntervalPolicyDecisionLedgerError(
            "Ledger contains duplicate decision IDs."
        )
    conflict_key = ["sensitivity_run_id", "target_candidate_id"]
    conflicts = entries.duplicated(subset=conflict_key, keep=False)
    if conflicts.any():
        raise IntervalPolicyDecisionLedgerError(
            "Ledger contains conflicting decisions for one sensitivity run and target candidate."
        )
    summary = pd.DataFrame(
        [
            {
                "ledger_run_id": run_id,
                "ledger_run_timestamp_utc": run_timestamp,
                "decision_count": int(len(entries)),
                "sensitivity_run_count": int(
                    entries["sensitivity_run_id"].nunique()
                ),
                "target_candidate_count": int(
                    entries[["sensitivity_run_id", "target_candidate_id"]]
                    .drop_duplicates()
                    .shape[0]
                ),
                "retain_active_policy_count": int(
                    (entries["decision"] == "retain_active_policy").sum()
                ),
                "reject_candidate_count": int(
                    (entries["decision"] == "reject_candidate").sum()
                ),
                "request_candidate_revision_count": int(
                    (
                        entries["decision"]
                        == "request_candidate_revision"
                    ).sum()
                ),
                "first_decision_timestamp_utc": entries[
                    "decision_timestamp_utc"
                ].min(),
                "last_decision_timestamp_utc": entries[
                    "decision_timestamp_utc"
                ].max(),
                "conflict_count": 0,
                "human_review_required": bool(
                    entries["follow_up_human_action_required"].any()
                ),
                "ledger_contract_version": LEDGER_CONTRACT_VERSION,
                **{field: False for field in LEDGER_SAFETY_FIELDS},
            }
        ]
    )
    verify_policy_decision_ledger(entries, summary)
    return entries, summary


def verify_policy_decision_ledger(
    entries: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    """Verify ledger ordering, uniqueness, counts, and no-action authority."""
    missing_entries = sorted(ENTRY_COLUMNS - set(entries.columns))
    missing_summary = sorted(SUMMARY_COLUMNS - set(summary.columns))
    if missing_entries or missing_summary:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger evidence is missing required columns."
        )
    if entries.empty or len(summary) != 1:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger requires entries and exactly one summary row."
        )
    if entries["ledger_run_id"].nunique() != 1:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger entries must share one ledger_run_id."
        )
    run_id = entries["ledger_run_id"].iloc[0]
    if summary["ledger_run_id"].iloc[0] != run_id:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger summary run identity is inconsistent."
        )
    if entries["decision_sequence"].tolist() != list(
        range(1, len(entries) + 1)
    ):
        raise IntervalPolicyDecisionLedgerError(
            "decision_sequence must be contiguous from 1."
        )
    if entries["decision_id"].duplicated().any():
        raise IntervalPolicyDecisionLedgerError(
            "Ledger contains duplicate decision IDs."
        )
    if entries.duplicated(
        subset=["sensitivity_run_id", "target_candidate_id"], keep=False
    ).any():
        raise IntervalPolicyDecisionLedgerError(
            "Ledger contains conflicting target decisions."
        )
    timestamps = pd.to_datetime(
        entries["decision_timestamp_utc"], utc=True, errors="coerce"
    )
    if timestamps.isna().any() or not timestamps.is_monotonic_increasing:
        raise IntervalPolicyDecisionLedgerError(
            "Ledger decision timestamps must be valid and ordered."
        )
    numeric_counts = {
        "decision_count": len(entries),
        "sensitivity_run_count": entries["sensitivity_run_id"].nunique(),
        "target_candidate_count": entries[
            ["sensitivity_run_id", "target_candidate_id"]
        ].drop_duplicates().shape[0],
        "retain_active_policy_count": (
            entries["decision"] == "retain_active_policy"
        ).sum(),
        "reject_candidate_count": (
            entries["decision"] == "reject_candidate"
        ).sum(),
        "request_candidate_revision_count": (
            entries["decision"] == "request_candidate_revision"
        ).sum(),
    }
    for field, expected in numeric_counts.items():
        observed = summary[field].iloc[0]
        if not isfinite(float(observed)) or int(observed) != int(expected):
            raise IntervalPolicyDecisionLedgerError(
                f"Ledger summary {field} is inconsistent."
            )
    if int(summary["conflict_count"].iloc[0]) != 0:
        raise IntervalPolicyDecisionLedgerError(
            "A valid ledger cannot retain unresolved conflicts."
        )
    for frame in (entries, summary):
        if set(frame["ledger_contract_version"]) != {
            LEDGER_CONTRACT_VERSION
        }:
            raise IntervalPolicyDecisionLedgerError(
                "Unsupported ledger contract version."
            )
        for field in LEDGER_SAFETY_FIELDS:
            values = frame[field]
            if values.dtype == bool:
                enabled = values.any()
            else:
                parsed = values.astype(str).str.lower().map(
                    {"true": True, "false": False}
                )
                enabled = parsed.isna().any() or parsed.any()
            if enabled:
                raise IntervalPolicyDecisionLedgerError(
                    f"Ledger safety field {field} must remain false."
                )


def ledger_digest(entries: pd.DataFrame, summary: pd.DataFrame) -> str:
    """Return a stable digest over verified ledger evidence."""
    verify_policy_decision_ledger(entries, summary)
    return _digest(
        {
            "entries": entries.to_dict(orient="records"),
            "summary": summary.to_dict(orient="records"),
        }
    )
