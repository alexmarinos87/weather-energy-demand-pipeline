from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

REPORT_CONTRACT_VERSION = "interval-health-report-v1"
TREND_CONTRACT_VERSION = "interval-health-trend-v1"
STATUS_RANK = {"healthy": 0, "warning": 1, "failed": 2}
SLICE_COLUMNS = [
    "scenario", "source_area", "resource_id", "city",
    "requested_horizon_minutes", "model_name", "feature_contract_version",
    "target_coverage_level", "interval_contract_version",
]
REQUIRED_RUN = {
    "trend_run_id", "scenario", "source_area", "resource_id", "city",
    "requested_horizon_minutes", "model_name", "target_coverage_level",
    "interval_run_id", "interval_run_timestamp_utc", "empirical_coverage_pct",
    "average_interval_width_mw", "calibration_observation_count",
    "monitoring_window", "trend_contract_version",
}
REQUIRED_SLICE = {
    "trend_run_id", "trend_run_timestamp_utc", *SLICE_COLUMNS,
    "monitor_run_id", "monitor_status", "failed_error_check_count",
    "failed_warning_check_count", "interval_run_count",
    "recent_interval_run_count", "reference_interval_run_count",
    "reference_history_sufficient", "latest_interval_run_id",
    "latest_interval_run_timestamp_utc", "latest_evaluation_end_utc",
    "latest_empirical_coverage_pct", "latest_coverage_shortfall_pct_points",
    "latest_average_interval_width_mw", "latest_calibration_observation_count",
    "recent_empirical_coverage_pct", "recent_average_interval_width_mw",
    "recent_mean_calibration_observation_count",
    "recent_minimum_calibration_observation_count",
    "reference_empirical_coverage_pct", "reference_average_interval_width_mw",
    "reference_mean_calibration_observation_count", "coverage_drop_pct_points",
    "average_interval_width_increase_pct", "calibration_history_drop_pct",
    "attention_required", "trend_contract_version",
}


class IntervalHealthReportError(ValueError):
    """Raised when retained trend evidence cannot support a thin report."""


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise IntervalHealthReportError(
            f"{label} is missing required columns: {', '.join(missing)}."
        )
    if frame.empty:
        raise IntervalHealthReportError(f"{label} must not be empty.")


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column].fillna("").astype(str).str.strip()
    if values.eq("").any():
        raise IntervalHealthReportError(f"{column} must contain non-empty values.")
    return values


def _number(
    frame: pd.DataFrame,
    column: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    allow_null: bool = False,
) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if not allow_null and values.isna().any():
        raise IntervalHealthReportError(f"{column} must contain numeric values.")
    finite = values.dropna()
    if not finite.map(lambda value: isfinite(float(value))).all():
        raise IntervalHealthReportError(f"{column} must contain finite values.")
    if minimum is not None and (finite < minimum).any():
        raise IntervalHealthReportError(f"{column} must be at least {minimum}.")
    if maximum is not None and (finite > maximum).any():
        raise IntervalHealthReportError(f"{column} must be at most {maximum}.")
    return values.astype(float)


def _bool(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame[column].dtype == bool:
        return frame[column].astype(bool)
    parsed = frame[column].astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}
    )
    if parsed.isna().any():
        raise IntervalHealthReportError(f"{column} must contain booleans.")
    return parsed.astype(bool)


def _prepare(
    run_trends: pd.DataFrame, slice_trends: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    _require(run_trends, REQUIRED_RUN, "Run trends")
    _require(slice_trends, REQUIRED_SLICE, "Slice trends")
    runs, slices = run_trends.copy(), slice_trends.copy()
    for frame, columns in (
        (runs, ("trend_run_id", "scenario", "source_area", "resource_id", "city",
                "model_name", "monitoring_window", "trend_contract_version")),
        (slices, ("trend_run_id", "scenario", "source_area", "resource_id", "city",
                  "model_name", "feature_contract_version", "interval_contract_version",
                  "monitor_run_id", "monitor_status", "trend_contract_version")),
    ):
        for column in columns:
            frame[column] = _text(frame, column)
    run_ids = set(runs["trend_run_id"]) | set(slices["trend_run_id"])
    if len(run_ids) != 1:
        raise IntervalHealthReportError(
            "Run and slice trends must bind to exactly one trend_run_id."
        )
    trend_run_id = next(iter(run_ids))
    if set(runs["trend_contract_version"]) != {TREND_CONTRACT_VERSION} or set(
        slices["trend_contract_version"]
    ) != {TREND_CONTRACT_VERSION}:
        raise IntervalHealthReportError(
            "Thin-client reporting requires interval-health-trend-v1 evidence."
        )
    if not set(slices["monitor_status"]).issubset(STATUS_RANK):
        raise IntervalHealthReportError("Slice trends contain an unsupported status.")
    if slices.groupby("scenario")["monitor_status"].nunique().max() != 1:
        raise IntervalHealthReportError(
            "A scenario has inconsistent retained monitor status."
        )
    for frame in (runs, slices):
        frame["requested_horizon_minutes"] = _number(
            frame, "requested_horizon_minutes", minimum=1
        ).astype(int)
        frame["target_coverage_level"] = _number(
            frame, "target_coverage_level", minimum=0, maximum=1
        )
        if not frame["target_coverage_level"].between(
            0, 1, inclusive="neither"
        ).all():
            raise IntervalHealthReportError(
                "target_coverage_level must be strictly between 0 and 1."
            )
    for column in (
        "latest_empirical_coverage_pct", "latest_coverage_shortfall_pct_points",
        "recent_empirical_coverage_pct",
    ):
        slices[column] = _number(slices, column, minimum=0, maximum=100)
    for column in (
        "latest_average_interval_width_mw", "recent_average_interval_width_mw",
        "recent_mean_calibration_observation_count",
        "recent_minimum_calibration_observation_count",
    ):
        slices[column] = _number(slices, column, minimum=0)
    for column in (
        "reference_empirical_coverage_pct", "reference_average_interval_width_mw",
        "reference_mean_calibration_observation_count", "coverage_drop_pct_points",
        "average_interval_width_increase_pct", "calibration_history_drop_pct",
    ):
        slices[column] = _number(slices, column, allow_null=True)
    for column in (
        "failed_error_check_count", "failed_warning_check_count", "interval_run_count",
        "recent_interval_run_count", "reference_interval_run_count",
        "latest_calibration_observation_count",
    ):
        slices[column] = _number(slices, column, minimum=0).astype(int)
    slices["reference_history_sufficient"] = _bool(
        slices, "reference_history_sufficient"
    )
    slices["attention_required"] = _bool(slices, "attention_required")
    for column in (
        "latest_interval_run_timestamp_utc", "latest_evaluation_end_utc",
        "trend_run_timestamp_utc",
    ):
        parsed = pd.to_datetime(slices[column], errors="coerce", utc=True)
        if parsed.isna().any():
            raise IntervalHealthReportError(f"{column} must contain timestamps.")
        slices[column] = parsed
    if slices.duplicated(subset=SLICE_COLUMNS, keep=False).any():
        raise IntervalHealthReportError(
            "Slice trends contain duplicate reporting identities."
        )
    if set(runs["scenario"]) != set(slices["scenario"]):
        raise IntervalHealthReportError(
            "Run and slice trends do not contain the same scenarios."
        )
    identity = [
        "scenario", "source_area", "resource_id", "city",
        "requested_horizon_minutes", "model_name", "target_coverage_level",
    ]
    run_set = set(runs[identity].drop_duplicates().itertuples(index=False, name=None))
    slice_set = set(slices[identity].itertuples(index=False, name=None))
    if run_set != slice_set:
        raise IntervalHealthReportError(
            "Run and slice trend identities do not match."
        )
    return runs, slices, trend_run_id


def _max(frame: pd.DataFrame, column: str) -> float | None:
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return None if values.empty else float(values.max())


def _base(report_run_id: str, timestamp: pd.Timestamp, group: pd.DataFrame):
    return {
        "report_run_id": report_run_id,
        "report_run_timestamp_utc": timestamp,
        "trend_run_id": str(group["trend_run_id"].iloc[0]),
        "monitor_status": str(group["monitor_status"].iloc[0]),
        "status_rank": STATUS_RANK[str(group["monitor_status"].iloc[0])],
        "model_count": int(group["model_name"].nunique()),
        "coverage_level_count": int(group["target_coverage_level"].nunique()),
        "slice_count": int(len(group)),
        "attention_slice_count": int(group["attention_required"].sum()),
        "failed_error_check_count": int(group["failed_error_check_count"].max()),
        "failed_warning_check_count": int(group["failed_warning_check_count"].max()),
        "maximum_latest_coverage_shortfall_pct_points": float(
            group["latest_coverage_shortfall_pct_points"].max()
        ),
        "maximum_average_interval_width_increase_pct": _max(
            group, "average_interval_width_increase_pct"
        ),
        "maximum_coverage_drop_pct_points": _max(group, "coverage_drop_pct_points"),
        "minimum_recent_calibration_observation_count": int(
            group["recent_minimum_calibration_observation_count"].min()
        ),
        "report_contract_version": REPORT_CONTRACT_VERSION,
    }


def _overview(slices: pd.DataFrame, run_id: str, timestamp: pd.Timestamp):
    rows = []
    for scenario, group in slices.groupby("scenario", sort=True):
        row = _base(run_id, timestamp, group)
        row.update({
            "scenario": scenario,
            "source_area_count": int(group["source_area"].nunique()),
            "horizon_count": int(group["requested_horizon_minutes"].nunique()),
            "attention_slice_pct": float(
                row["attention_slice_count"] / row["slice_count"] * 100.0
            ),
            "maximum_calibration_history_drop_pct": _max(
                group, "calibration_history_drop_pct"
            ),
            "reference_history_sufficient_slice_count": int(
                group["reference_history_sufficient"].sum()
            ),
        })
        rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["status_rank", "scenario"], ascending=[False, True]
    ).reset_index(drop=True)


def _area_horizon(slices: pd.DataFrame, run_id: str, timestamp: pd.Timestamp):
    rows = []
    grouping = [
        "scenario", "source_area", "resource_id", "city",
        "requested_horizon_minutes",
    ]
    for identity, group in slices.groupby(grouping, sort=True, dropna=False):
        scenario, area, resource, city, horizon = identity
        row = _base(run_id, timestamp, group)
        row.pop("failed_error_check_count", None)
        row.pop("failed_warning_check_count", None)
        row.update({
            "scenario": scenario, "source_area": area, "resource_id": resource,
            "city": city, "requested_horizon_minutes": int(horizon),
        })
        rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["status_rank", "scenario", "source_area", "requested_horizon_minutes"],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)


def _attention(slices: pd.DataFrame, run_id: str, timestamp: pd.Timestamp):
    queue = slices.loc[
        slices["attention_required"] | (slices["monitor_status"] != "healthy")
    ].copy()
    columns = [
        "report_run_id", "report_run_timestamp_utc", "trend_run_id",
        *SLICE_COLUMNS, "monitor_status", "status_rank", "latest_interval_run_id",
        "latest_empirical_coverage_pct", "latest_coverage_shortfall_pct_points",
        "latest_average_interval_width_mw", "latest_calibration_observation_count",
        "coverage_drop_pct_points", "average_interval_width_increase_pct",
        "calibration_history_drop_pct", "report_contract_version",
    ]
    if queue.empty:
        return pd.DataFrame(columns=columns)
    queue.insert(0, "report_run_id", run_id)
    queue.insert(1, "report_run_timestamp_utc", timestamp)
    queue["status_rank"] = queue["monitor_status"].map(STATUS_RANK).astype(int)
    queue["report_contract_version"] = REPORT_CONTRACT_VERSION
    return queue[columns].sort_values(
        ["status_rank", "latest_coverage_shortfall_pct_points",
         "average_interval_width_increase_pct", "source_area",
         "requested_horizon_minutes", "model_name", "target_coverage_level"],
        ascending=[False, False, False, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)


def _markdown(overview, area, attention) -> str:
    lines = [
        "# Interval-health trend report", "",
        "This thin-client report reads retained `interval-health-trend-v1` datasets. "
        "It does not recalculate monitor status, interval radii, or thresholds.", "",
        "## Scenario overview", "",
        "| Scenario | Status | Areas | Horizons | Slices | Attention | Max shortfall | Max width increase | Min calibration |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in overview.itertuples(index=False):
        width = "n/a" if pd.isna(row.maximum_average_interval_width_increase_pct) else f"{row.maximum_average_interval_width_increase_pct:.2f}%"
        lines.append(
            f"| {row.scenario} | {row.monitor_status} | {row.source_area_count} | "
            f"{row.horizon_count} | {row.slice_count} | {row.attention_slice_count} | "
            f"{row.maximum_latest_coverage_shortfall_pct_points:.2f} pp | {width} | "
            f"{row.minimum_recent_calibration_observation_count} |"
        )
    lines += ["", "## Area and horizon triage", "",
              "| Scenario | Area | City | Horizon | Status | Slices | Attention |",
              "| --- | --- | --- | ---: | --- | ---: | ---: |"]
    for row in area.itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.source_area} | {row.city} | "
            f"{row.requested_horizon_minutes} | {row.monitor_status} | "
            f"{row.slice_count} | {row.attention_slice_count} |"
        )
    lines += ["", "## Attention queue", ""]
    if attention.empty:
        lines.append("No retained slice currently requires attention.")
    else:
        lines += [
            "| Status | Scenario | Area | Horizon | Model | Coverage | Shortfall |",
            "| --- | --- | --- | ---: | --- | ---: | ---: |",
        ]
        for row in attention.head(50).itertuples(index=False):
            lines.append(
                f"| {row.monitor_status} | {row.scenario} | {row.source_area} | "
                f"{row.requested_horizon_minutes} | {row.model_name} | "
                f"{row.target_coverage_level * 100:.1f}% | "
                f"{row.latest_coverage_shortfall_pct_points:.2f} pp |"
            )
    lines += [
        "", "Empirical coverage is retrospective evidence, not an unconditional future guarantee.",
        "", "This report is advisory only. It performs no automatic recalibration, "
        "model change, schedule change, promotion, remediation, alert delivery, "
        "deployment, or publication.", "",
    ]
    return "\n".join(lines)


def _html(overview, area, attention) -> str:
    attention_html = (
        "<p>No retained slice currently requires attention.</p>"
        if attention.empty
        else attention.head(100).to_html(index=False, border=0, escape=True)
    )
    return "".join([
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<title>Interval-health trend report</title></head><body>",
        "<h1>Interval-health trend report</h1>",
        "<p>This thin client renders retained interval-health-trend-v1 evidence. "
        "It does not recalculate monitor status or interval radii.</p>",
        "<h2>Scenario overview</h2>", overview.to_html(index=False, border=0),
        "<h2>Area and horizon triage</h2>", area.to_html(index=False, border=0),
        "<h2>Attention queue</h2>", attention_html,
        "<p><strong>Boundary:</strong> empirical coverage is retrospective evidence. "
        "No automatic recalibration, model, schedule, promotion, remediation, "
        "alert, deployment, or publication action is performed.</p></body></html>",
    ])


def build_interval_health_report(
    run_trends: pd.DataFrame,
    slice_trends: pd.DataFrame,
    *,
    report_run_id: str | None = None,
    report_run_timestamp: datetime | pd.Timestamp | str | None = None,
) -> dict[str, Any]:
    """Build retained thin-client datasets plus Markdown and HTML."""
    runs, slices, trend_run_id = _prepare(run_trends, slice_trends)
    run_id = report_run_id or "ihr-" + uuid4().hex[:24]
    if not (
        len(run_id) == 28 and run_id.startswith("ihr-")
        and all(character in "0123456789abcdef" for character in run_id[4:])
    ):
        raise IntervalHealthReportError(
            "report_run_id must match ihr- plus 24 lowercase hexadecimal characters."
        )
    timestamp = pd.Timestamp(report_run_timestamp or datetime.now(timezone.utc))
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalHealthReportError("report_run_timestamp must be timezone-aware.")
    timestamp = timestamp.tz_convert("UTC")
    overview = _overview(slices, run_id, timestamp)
    area = _area_horizon(slices, run_id, timestamp)
    attention = _attention(slices, run_id, timestamp)
    return {
        "frames": {
            "interval_health_report_overview": overview,
            "interval_health_report_area_horizon": area,
            "interval_health_report_attention_queue": attention,
        },
        "markdown": _markdown(overview, area, attention),
        "html": _html(overview, area, attention),
        "metadata": {
            "report_run_id": run_id,
            "report_run_timestamp_utc": timestamp,
            "source_trend_run_id": trend_run_id,
            "run_trend_row_count": int(len(runs)),
            "slice_trend_row_count": int(len(slices)),
            "report_contract_version": REPORT_CONTRACT_VERSION,
            "automatic_recalibration_performed": False,
            "automatic_model_change_performed": False,
            "automatic_schedule_change_performed": False,
            "automatic_promotion_performed": False,
            "alert_delivery_performed": False,
        },
    }


def read_frame(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise IntervalHealthReportError(
        f"Unsupported trend file type for {path.name}; use CSV or Parquet."
    )


def write_frame_atomic(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
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
            raise IntervalHealthReportError("output_format must be csv or parquet.")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def write_text_atomic(content: str, path: Path) -> Path:
    if not content.strip():
        raise IntervalHealthReportError(f"{path.name} must not be empty.")
    temporary = path.with_name(f".{path.name}.tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path
