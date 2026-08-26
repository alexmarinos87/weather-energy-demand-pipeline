from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from forecasting._interval_policy_decision_ledger_core import (
    build_policy_decision_ledger,
    ledger_digest,
    verify_policy_decision_ledger,
)
from forecasting._interval_policy_decision_ledger_common import (
    LEDGER_CONTRACT_VERSION,
    LEDGER_SAFETY_FIELDS,
    IntervalPolicyDecisionLedgerError,
)


def read_frame(path: Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicyDecisionLedgerError(
        f"Unsupported summary format for {path}; use CSV or Parquet."
    )


def load_decision_bindings(
    manifest_path: Path,
) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    """Load decision/summary pairs from one explicit local manifest."""
    manifest_path = Path(manifest_path)
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntervalPolicyDecisionLedgerError(
            "Decision binding manifest must be readable JSON."
        ) from exc
    if not isinstance(document, dict) or not isinstance(
        document.get("bindings"), list
    ):
        raise IntervalPolicyDecisionLedgerError(
            "Decision binding manifest requires a bindings array."
        )
    if not document["bindings"]:
        raise IntervalPolicyDecisionLedgerError(
            "Decision binding manifest must contain at least one binding."
        )
    bindings: list[tuple[dict[str, Any], pd.DataFrame]] = []
    seen: set[tuple[Path, Path]] = set()
    for item in document["bindings"]:
        if not isinstance(item, dict):
            raise IntervalPolicyDecisionLedgerError(
                "Each decision binding must be a JSON object."
            )
        decision_name = str(item.get("decision", "")).strip()
        summary_name = str(item.get("sensitivity_summary", "")).strip()
        if not decision_name or not summary_name:
            raise IntervalPolicyDecisionLedgerError(
                "Each binding requires decision and sensitivity_summary paths."
            )
        decision_path = (manifest_path.parent / decision_name).resolve()
        summary_path = (manifest_path.parent / summary_name).resolve()
        identity = (decision_path, summary_path)
        if identity in seen:
            raise IntervalPolicyDecisionLedgerError(
                "Decision binding manifest contains duplicate bindings."
            )
        seen.add(identity)
        try:
            decision = json.loads(decision_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise IntervalPolicyDecisionLedgerError(
                f"Decision binding {decision_path} is not readable JSON."
            ) from exc
        bindings.append((decision, read_frame(summary_path)))
    return bindings


def render_policy_decision_ledger(
    entries: pd.DataFrame,
    summary: pd.DataFrame,
) -> str:
    """Render one human-readable decision-ledger report."""
    verify_policy_decision_ledger(entries, summary)
    row = summary.iloc[0]
    lines = [
        "# Interval-policy decision ledger",
        "",
        f"- Ledger run: `{row['ledger_run_id']}`",
        f"- Decision count: {int(row['decision_count'])}",
        f"- Sensitivity runs: {int(row['sensitivity_run_count'])}",
        f"- Conflicts: {int(row['conflict_count'])}",
        f"- Human follow-up required: {bool(row['human_review_required'])}",
        "",
        "| Sequence | Decision | Target | Reviewer | Ticket | Timestamp |",
        "| ---: | --- | --- | --- | --- | --- |",
    ]
    for item in entries.itertuples(index=False):
        lines.append(
            f"| {item.decision_sequence} | {item.decision} | "
            f"{item.target_candidate_id} | {item.reviewer_name} | "
            f"{item.review_ticket} | "
            f"{pd.Timestamp(item.decision_timestamp_utc).isoformat()} |"
        )
    lines.extend(
        [
            "",
            "This append-only ledger verifies retained decision evidence and rejects duplicate or conflicting decisions.",
            "It does not activate thresholds, update the active policy, recalibrate intervals, change models or schedules, deliver alerts, deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_frame_atomic(
    frame: pd.DataFrame,
    path: Path,
    output_format: str,
) -> None:
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
            raise IntervalPolicyDecisionLedgerError(
                "output_format must be csv or parquet."
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_policy_decision_ledger(
    output_directory: Path,
    entries: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    output_format: str = "parquet",
) -> tuple[Path, Path, Path, Path]:
    """Write immutable tabular, Markdown, and manifest evidence."""
    verify_policy_decision_ledger(entries, summary)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    run_id = summary["ledger_run_id"].iloc[0]
    extension = "csv" if output_format == "csv" else "parquet"
    entries_path = output_directory / (
        f"interval_policy_decision_ledger_{run_id}.{extension}"
    )
    summary_path = output_directory / (
        f"interval_policy_decision_ledger_summary_{run_id}.{extension}"
    )
    report_path = output_directory / (
        f"interval_policy_decision_ledger_{run_id}.md"
    )
    manifest_path = output_directory / (
        f"interval_policy_decision_ledger_{run_id}.json"
    )
    temporary_text = [
        report_path.with_name(f".{report_path.name}.tmp"),
        manifest_path.with_name(f".{manifest_path.name}.tmp"),
    ]
    for candidate in (report_path, manifest_path, *temporary_text):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    _write_frame_atomic(entries, entries_path, output_format)
    try:
        _write_frame_atomic(summary, summary_path, output_format)
        temporary_text[0].write_text(
            render_policy_decision_ledger(entries, summary),
            encoding="utf-8",
        )
        manifest = {
            "ledger_run_id": run_id,
            "ledger_contract_version": LEDGER_CONTRACT_VERSION,
            "ledger_sha256": ledger_digest(entries, summary),
            "entries_path": entries_path.name,
            "summary_path": summary_path.name,
            "report_path": report_path.name,
            **{field: False for field in LEDGER_SAFETY_FIELDS},
        }
        temporary_text[1].write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_text[0].replace(report_path)
        temporary_text[1].replace(manifest_path)
    except Exception:
        entries_path.unlink(missing_ok=True)
        summary_path.unlink(missing_ok=True)
        raise
    finally:
        for temporary in temporary_text:
            temporary.unlink(missing_ok=True)
    return entries_path, summary_path, report_path, manifest_path


__all__ = [
    "LEDGER_CONTRACT_VERSION",
    "LEDGER_SAFETY_FIELDS",
    "IntervalPolicyDecisionLedgerError",
    "build_policy_decision_ledger",
    "ledger_digest",
    "load_decision_bindings",
    "read_frame",
    "render_policy_decision_ledger",
    "verify_policy_decision_ledger",
    "write_policy_decision_ledger",
]
