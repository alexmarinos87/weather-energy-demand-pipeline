from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from forecasting.interval_monitoring import (
    FAILED_STATUS,
    HEALTHY_STATUS,
    MONITORING_VERSION,
    POLICY_VERSION,
    WARNING_STATUS,
    PredictionIntervalMonitoringConfig,
    monitor_prediction_interval_health,
)
from forecasting.portfolio_demo import (
    _artifact,
    _canonical,
    _document_hash,
    _file_hash,
    _read_frame,
    _utc_timestamp,
    _write_frame,
    _write_text,
)
from forecasting.portfolio_intervals import (
    INTERVAL_CONTRACT_VERSION,
    INTERVAL_COVERAGE_LEVELS,
    MIN_INTERVAL_CALIBRATION_ROWS,
    build_portfolio_interval_evidence,
)
from forecasting.portfolio_seasonal import build_portfolio_seasonal_evidence
from forecasting.seasonal_baselines import ALL_MODELS
from ingestion.common.source_area import load_source_area_contract


MANIFEST_CONTRACT_VERSION = "portfolio-interval-health-manifest-v1"
HEALTH_DEMO_RUN_ID_PATTERN = re.compile(r"^pih-[0-9a-f]{24}$")
EXPECTED_SOURCE_AREAS = {
    "east_midlands",
    "south_wales",
    "south_west",
    "west_midlands",
}
SCENARIOS = (HEALTHY_STATUS, WARNING_STATUS, FAILED_STATUS)
EXPECTED_STATUS_BY_SCENARIO = {status: status for status in SCENARIOS}
HISTORY_RUNS_PER_SCENARIO = 9
HEALTH_ARTIFACT_ROLES = {
    "repeated_interval_metric_history",
    "prediction_interval_health_checks",
    "prediction_interval_health_summary",
    "interval_health_operator_report",
}
MONITORING_CONFIG = PredictionIntervalMonitoringConfig()
MONITORING_INPUT_COLUMNS = [
    "interval_run_id",
    "interval_run_timestamp_utc",
    "source_area",
    "resource_id",
    "city",
    "requested_horizon_minutes",
    "model_name",
    "feature_contract_version",
    "target_coverage_level",
    "calibration_observation_count",
    "calibration_radius_mw",
    "evaluation_end_utc",
    "evaluation_observation_count",
    "empirical_coverage_pct",
    "average_interval_width_mw",
    "interval_contract_version",
]
SUMMARY_AUTHORITY_FIELDS = (
    "automatic_remediation_allowed",
    "automatic_recalibration_allowed",
    "automatic_model_change_allowed",
    "automatic_schedule_change_allowed",
    "automatic_promotion_allowed",
)
MANIFEST_SAFETY = {
    "credential_free": True,
    "live_source_calls_performed": False,
    "fabric_operations_performed": False,
    "schedule_activation_performed": False,
    "automatic_remediation_performed": False,
    "automatic_recalibration_performed": False,
    "automatic_model_change_performed": False,
    "automatic_promotion_performed": False,
    "alert_delivery_performed": False,
    "external_publication_performed": False,
}


class PortfolioIntervalHealthError(ValueError):
    """Raised when repeated portfolio interval-health evidence is invalid."""


def _source_bindings(frame: pd.DataFrame) -> list[dict[str, str]]:
    required = {"source_area", "resource_id", "city"}
    if not required.issubset(frame.columns):
        raise PortfolioIntervalHealthError(
            "Interval metrics do not retain source-area identity."
        )
    bindings = [
        {
            "source_area": str(row.source_area),
            "resource_id": str(row.resource_id),
            "city": str(row.city),
        }
        for row in frame[["source_area", "resource_id", "city"]]
        .drop_duplicates()
        .sort_values(["source_area", "resource_id", "city"])
        .itertuples(index=False)
    ]
    if len(bindings) != 4 or {
        item["source_area"] for item in bindings
    } != EXPECTED_SOURCE_AREAS:
        raise PortfolioIntervalHealthError(
            "Repeated interval health evidence must retain four contracted areas."
        )
    if len({item["resource_id"] for item in bindings}) != 4 or len(
        {item["city"] for item in bindings}
    ) != 4:
        raise PortfolioIntervalHealthError(
            "Repeated interval health evidence has duplicated source identity."
        )
    return bindings


def _validate_base_metrics(metrics: pd.DataFrame) -> None:
    if metrics.empty:
        raise PortfolioIntervalHealthError("Base interval metrics are empty.")
    expectations = {
        "source_area": EXPECTED_SOURCE_AREAS,
        "requested_horizon_minutes": {30, 60},
        "model_name": set(ALL_MODELS),
        "target_coverage_level": set(INTERVAL_COVERAGE_LEVELS),
    }
    for column, expected in expectations.items():
        values = set(metrics[column])
        if column == "requested_horizon_minutes":
            values = {int(value) for value in values}
        elif column == "target_coverage_level":
            values = {float(value) for value in values}
        else:
            values = {str(value) for value in values}
        if values != expected:
            raise PortfolioIntervalHealthError(
                f"Base interval metrics have incomplete {column}."
            )
    identity = [
        "source_area",
        "resource_id",
        "city",
        "requested_horizon_minutes",
        "model_name",
        "feature_contract_version",
        "target_coverage_level",
        "interval_contract_version",
    ]
    if metrics.duplicated(subset=identity, keep=False).any():
        raise PortfolioIntervalHealthError(
            "Base interval metrics contain duplicate monitoring slices."
        )


def _scenario_metric_history(
    base_metrics: pd.DataFrame,
    *,
    scenario: str,
    run_id: str,
    anchor_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    if scenario not in SCENARIOS:
        raise PortfolioIntervalHealthError(f"Unsupported scenario {scenario!r}.")
    rows: list[pd.DataFrame] = []
    reference_count = (
        HISTORY_RUNS_PER_SCENARIO - MONITORING_CONFIG.recent_interval_run_count
    )
    for index in range(HISTORY_RUNS_PER_SCENARIO):
        current = base_metrics[MONITORING_INPUT_COLUMNS].copy()
        interval_timestamp = anchor_timestamp - pd.Timedelta(
            hours=HISTORY_RUNS_PER_SCENARIO - index
        )
        reference_window = index < reference_count
        nominal = current["target_coverage_level"].astype(float) * 100.0
        base_width = current["average_interval_width_mw"].astype(float).clip(
            lower=1.0
        )
        base_calibration = current["calibration_observation_count"].astype(
            int
        ).clip(lower=30)
        if scenario == HEALTHY_STATUS:
            empirical, width, calibration = nominal - 2.0, base_width, base_calibration
        elif scenario == WARNING_STATUS:
            empirical = nominal - (1.0 if reference_window else 3.0)
            width = base_width * (1.0 if reference_window else 1.35)
            calibration = base_calibration
        else:
            empirical = nominal - (1.0 if reference_window else 10.0)
            width = base_width
            calibration = (
                base_calibration
                if reference_window
                else pd.Series(20, index=current.index, dtype="int64")
            )
        current["scenario"] = scenario
        current["history_sequence"] = index + 1
        current["interval_run_id"] = (
            f"{run_id}-{scenario}-interval-{index + 1:02d}"
        )
        current["interval_run_timestamp_utc"] = interval_timestamp
        current["evaluation_end_utc"] = interval_timestamp - pd.Timedelta(
            minutes=30
        )
        current["empirical_coverage_pct"] = empirical.clip(0, 100)
        current["average_interval_width_mw"] = width
        current["calibration_radius_mw"] = width / 2.0
        current["calibration_observation_count"] = calibration
        rows.append(current)
    return pd.concat(rows, ignore_index=True).sort_values(
        [
            "scenario",
            "interval_run_timestamp_utc",
            "source_area",
            "requested_horizon_minutes",
            "model_name",
            "target_coverage_level",
        ]
    ).reset_index(drop=True)


def _operator_report(summaries: pd.DataFrame, checks: pd.DataFrame) -> str:
    lines = [
        "# Portfolio interval-health operator report",
        "",
        "This credential-free report demonstrates repeated interval-health "
        "evidence for every contracted source area. Healthy, warning, and "
        "failed outcomes are deterministic review scenarios, not live alerts.",
        "",
        "| Scenario | Status | Checks | Failed errors | Failed warnings | Slices | Retained runs |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summaries.sort_values("scenario").itertuples(index=False):
        lines.append(
            f"| {row.scenario} | {row.monitor_status} | {int(row.check_count)} | "
            f"{int(row.failed_error_check_count)} | "
            f"{int(row.failed_warning_check_count)} | "
            f"{int(row.monitored_interval_slice_count)} | "
            f"{int(row.retained_interval_run_count)} |"
        )
    lines.extend(
        [
            "",
            "## Area-level triage",
            "",
            "| Scenario | Source area | Failed errors | Failed warnings |",
            "| --- | --- | ---: | ---: |",
        ]
    )
    failed_checks = checks.loc[~checks["passed"].astype(bool)]
    for scenario in SCENARIOS:
        for area in sorted(EXPECTED_SOURCE_AREAS):
            subset = failed_checks.loc[
                (failed_checks["scenario"] == scenario)
                & (failed_checks["source_area"] == area)
            ]
            lines.append(
                f"| {scenario} | {area} | "
                f"{int((subset['severity'] == 'error').sum())} | "
                f"{int((subset['severity'] == 'warning').sum())} |"
            )
    lines.extend(
        [
            "",
            "Monitoring evidence is advisory only. Automatic recalibration, "
            "model changes, schedule changes, promotion changes, remediation, "
            "and alert delivery are not authorised or performed.",
            "",
            "Empirical interval coverage is retrospective evidence and is not "
            "an unconditional future guarantee under distribution shift.",
            "",
        ]
    )
    return "\n".join(lines)


def build_portfolio_interval_health_evidence(
    *,
    run_id: str,
    run_timestamp: datetime,
) -> dict[str, Any]:
    """Build repeated four-area interval histories and monitor outcomes."""
    timestamp = _utc_timestamp(run_timestamp, "run_timestamp")
    seasonal = build_portfolio_seasonal_evidence(
        source_areas=tuple(sorted(EXPECTED_SOURCE_AREAS)),
        run_id=f"{run_id}-source",
        run_timestamp=timestamp.to_pydatetime(),
    )
    intervals = build_portfolio_interval_evidence(
        seasonal["frames"]["seasonal_utc_predictions"],
        run_id=f"{run_id}-source",
        run_timestamp=timestamp.to_pydatetime(),
    )
    base_metrics = intervals["frames"]["prediction_interval_metrics"].copy()
    _validate_base_metrics(base_metrics)
    histories, check_frames, summary_frames = [], [], []
    for scenario in SCENARIOS:
        history = _scenario_metric_history(
            base_metrics,
            scenario=scenario,
            run_id=run_id,
            anchor_timestamp=timestamp,
        )
        checks, summary = monitor_prediction_interval_health(
            history,
            config=MONITORING_CONFIG,
            as_of_utc=timestamp,
            run_id=f"{run_id}-{scenario}-monitor",
            run_timestamp=timestamp,
        )
        checks.insert(0, "scenario", scenario)
        summary.insert(0, "scenario", scenario)
        observed = str(summary.loc[0, "monitor_status"])
        if observed != EXPECTED_STATUS_BY_SCENARIO[scenario]:
            raise PortfolioIntervalHealthError(
                f"Scenario {scenario} produced unexpected status {observed}."
            )
        histories.append(history)
        check_frames.append(checks)
        summary_frames.append(summary)
    history_frame = pd.concat(histories, ignore_index=True)
    checks_frame = pd.concat(check_frames, ignore_index=True)
    summary_frame = pd.concat(summary_frames, ignore_index=True)
    return {
        "frames": {
            "repeated_interval_metric_history": history_frame,
            "prediction_interval_health_checks": checks_frame,
            "prediction_interval_health_summary": summary_frame,
        },
        "markdown": _operator_report(summary_frame, checks_frame),
        "manifest": {
            "interval_health_scenarios": list(SCENARIOS),
            "interval_health_expected_status_by_scenario": dict(
                EXPECTED_STATUS_BY_SCENARIO
            ),
            "interval_history_runs_per_scenario": HISTORY_RUNS_PER_SCENARIO,
            "interval_health_models": sorted(ALL_MODELS),
            "interval_health_horizons_minutes": [30, 60],
            "interval_health_coverage_levels": list(INTERVAL_COVERAGE_LEVELS),
            "interval_health_source_interval_contract_version": (
                INTERVAL_CONTRACT_VERSION
            ),
            "interval_health_policy_version": POLICY_VERSION,
            "interval_health_monitoring_contract_version": MONITORING_VERSION,
            "minimum_interval_calibration_rows": MIN_INTERVAL_CALIBRATION_ROWS,
        },
    }


def _verify_artifacts(
    manifest: dict[str, Any], run_directory: Path
) -> dict[str, Path]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise PortfolioIntervalHealthError("Health artifacts must be a list.")
    roles = [str(item.get("artifact_role", "")) for item in artifacts]
    if set(roles) != HEALTH_ARTIFACT_ROLES or len(roles) != len(set(roles)):
        raise PortfolioIntervalHealthError(
            "Health artifact roles are incomplete or duplicated."
        )
    by_role: dict[str, Path] = {}
    for artifact in artifacts:
        relative = Path(str(artifact.get("relative_path", "")))
        if relative.is_absolute() or len(relative.parts) != 1:
            raise PortfolioIntervalHealthError(
                "Health artifact paths must be local filenames."
            )
        path = Path(run_directory) / relative
        if not path.is_file() or path.is_symlink():
            raise PortfolioIntervalHealthError(f"Health artifact is missing: {relative}.")
        if path.stat().st_size != int(artifact.get("size_bytes", -1)):
            raise PortfolioIntervalHealthError(
                f"Health artifact size changed: {relative}."
            )
        if _file_hash(path) != artifact.get("sha256"):
            raise PortfolioIntervalHealthError(
                f"Health artifact hash changed: {relative}."
            )
        row_count = artifact.get("row_count")
        if row_count is None:
            if path.suffix.lower() != ".md":
                raise PortfolioIntervalHealthError(
                    f"Only Markdown may omit row count: {relative}."
                )
        elif len(_read_frame(path)) != int(row_count):
            raise PortfolioIntervalHealthError(
                f"Health artifact row count changed: {relative}."
            )
        by_role[str(artifact["artifact_role"])] = path
    return by_role


def verify_portfolio_interval_health_manifest(
    manifest: dict[str, Any], run_directory: Path
) -> None:
    """Reopen and verify the repeated interval-health evidence bundle."""
    if not isinstance(manifest, dict):
        raise PortfolioIntervalHealthError("Health manifest must be a JSON object.")
    run_id = str(manifest.get("health_demo_run_id", ""))
    if not HEALTH_DEMO_RUN_ID_PATTERN.fullmatch(run_id):
        raise PortfolioIntervalHealthError("Health demo run ID is malformed.")
    run_timestamp = _utc_timestamp(
        manifest.get("health_demo_run_timestamp_utc"),
        "health_demo_run_timestamp_utc",
    )
    if manifest.get("manifest_contract_version") != MANIFEST_CONTRACT_VERSION:
        raise PortfolioIntervalHealthError("Unsupported health manifest contract.")
    if manifest.get("manifest_hash") != _document_hash(manifest):
        raise PortfolioIntervalHealthError("Health manifest hash is invalid.")
    if manifest.get("source_mode") != "deterministic_credential_free_demo":
        raise PortfolioIntervalHealthError("Health demo source mode is invalid.")
    if manifest.get("output_format") not in {"csv", "parquet"}:
        raise PortfolioIntervalHealthError("Health demo output format is invalid.")
    bindings = manifest.get("source_bindings")
    if manifest.get("source_groups") != 4 or not isinstance(bindings, list):
        raise PortfolioIntervalHealthError("Health demo source bindings are invalid.")
    if {str(item.get("source_area", "")) for item in bindings} != (
        EXPECTED_SOURCE_AREAS
    ):
        raise PortfolioIntervalHealthError("Health demo source areas are incomplete.")
    expected_fields = {
        "interval_health_scenarios": list(SCENARIOS),
        "interval_health_expected_status_by_scenario": dict(
            EXPECTED_STATUS_BY_SCENARIO
        ),
        "interval_history_runs_per_scenario": HISTORY_RUNS_PER_SCENARIO,
        "interval_health_models": sorted(ALL_MODELS),
        "interval_health_horizons_minutes": [30, 60],
        "interval_health_coverage_levels": list(INTERVAL_COVERAGE_LEVELS),
        "interval_health_source_interval_contract_version": INTERVAL_CONTRACT_VERSION,
        "interval_health_policy_version": POLICY_VERSION,
        "interval_health_monitoring_contract_version": MONITORING_VERSION,
        "minimum_interval_calibration_rows": MIN_INTERVAL_CALIBRATION_ROWS,
        **MANIFEST_SAFETY,
    }
    for field, expected in expected_fields.items():
        if manifest.get(field) != expected:
            raise PortfolioIntervalHealthError(
                f"Health manifest field {field} is invalid."
            )
    by_role = _verify_artifacts(manifest, Path(run_directory))
    history = _read_frame(by_role["repeated_interval_metric_history"])
    checks = _read_frame(by_role["prediction_interval_health_checks"])
    summaries = _read_frame(by_role["prediction_interval_health_summary"])
    expectations = {
        "scenario": set(SCENARIOS),
        "source_area": EXPECTED_SOURCE_AREAS,
        "requested_horizon_minutes": {30, 60},
        "model_name": set(ALL_MODELS),
        "target_coverage_level": set(INTERVAL_COVERAGE_LEVELS),
    }
    for column, expected in expectations.items():
        values = set(history[column])
        if column == "requested_horizon_minutes":
            values = {int(value) for value in values}
        elif column == "target_coverage_level":
            values = {float(value) for value in values}
        else:
            values = {str(value) for value in values}
        if values != expected:
            raise PortfolioIntervalHealthError(
                f"Health history has incomplete {column}."
            )
    expected_slice_count = 4 * 2 * len(ALL_MODELS) * len(INTERVAL_COVERAGE_LEVELS)
    for scenario, frame in history.groupby("scenario", sort=True):
        run_sizes = frame.groupby("interval_run_id").size()
        if len(run_sizes) != HISTORY_RUNS_PER_SCENARIO or set(run_sizes) != {
            expected_slice_count
        }:
            raise PortfolioIntervalHealthError(
                f"Scenario {scenario} does not retain nine complete interval runs."
            )
    if set(checks["scenario"].astype(str)) != set(SCENARIOS) or set(
        checks["source_area"].astype(str)
    ) != EXPECTED_SOURCE_AREAS:
        raise PortfolioIntervalHealthError("Health checks are incomplete.")
    if len(summaries) != len(SCENARIOS):
        raise PortfolioIntervalHealthError("Health summaries are incomplete.")
    statuses = dict(zip(summaries["scenario"], summaries["monitor_status"]))
    if statuses != EXPECTED_STATUS_BY_SCENARIO:
        raise PortfolioIntervalHealthError("Health summary statuses are invalid.")
    for field in SUMMARY_AUTHORITY_FIELDS:
        values = summaries[field].astype(str).str.lower().map(
            {"true": True, "false": False}
        )
        if values.isna().any() or values.any():
            raise PortfolioIntervalHealthError(
                f"Health summary authority field {field} is invalid."
            )
    for scenario in SCENARIOS:
        scenario_history = history.loc[history["scenario"] == scenario].drop(
            columns=["scenario"], errors="ignore"
        )
        retained_summary = summaries.loc[summaries["scenario"] == scenario].iloc[0]
        rerun_checks, rerun_summary = monitor_prediction_interval_health(
            scenario_history,
            config=MONITORING_CONFIG,
            as_of_utc=run_timestamp,
            run_id=str(retained_summary["monitor_run_id"]),
            run_timestamp=run_timestamp,
        )
        retained_checks = checks.loc[checks["scenario"] == scenario]
        if (
            str(rerun_summary.loc[0, "monitor_status"])
            != EXPECTED_STATUS_BY_SCENARIO[scenario]
            or len(rerun_checks) != len(retained_checks)
        ):
            raise PortfolioIntervalHealthError(
                f"Scenario {scenario} monitoring evidence cannot be reproduced."
            )
    report = by_role["interval_health_operator_report"].read_text(
        encoding="utf-8"
    )
    for token in (*SCENARIOS, *sorted(EXPECTED_SOURCE_AREAS)):
        if token not in report:
            raise PortfolioIntervalHealthError(f"Operator report is missing {token}.")
    for boundary in (
        "Automatic recalibration",
        "model changes",
        "schedule changes",
        "promotion changes",
        "not an unconditional future guarantee",
    ):
        if boundary not in report:
            raise PortfolioIntervalHealthError(
                f"Operator report is missing authority boundary {boundary}."
            )


def run_portfolio_interval_health_demo(
    output_root: Path,
    *,
    output_format: str = "csv",
    run_id: str | None = None,
    run_timestamp: Any | None = None,
) -> tuple[dict[str, Any], Path]:
    """Write one immutable repeated interval-health bundle atomically."""
    if output_format not in {"csv", "parquet"}:
        raise PortfolioIntervalHealthError("output_format must be csv or parquet.")
    run_id = run_id or "pih-" + uuid4().hex[:24]
    if not HEALTH_DEMO_RUN_ID_PATTERN.fullmatch(run_id):
        raise PortfolioIntervalHealthError(
            "run_id must match pih- plus 24 lowercase hexadecimal characters."
        )
    timestamp = _utc_timestamp(
        run_timestamp or datetime.now(timezone.utc), "run_timestamp"
    )
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    final_directory = output_root / run_id
    temporary_directory = output_root / f".{run_id}.tmp"
    for candidate in (final_directory, temporary_directory):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    temporary_directory.mkdir()
    try:
        evidence = build_portfolio_interval_health_evidence(
            run_id=run_id,
            run_timestamp=timestamp.to_pydatetime(),
        )
        history = evidence["frames"]["repeated_interval_metric_history"]
        artifacts = []
        for role, frame in evidence["frames"].items():
            path = _write_frame(frame, temporary_directory, role, output_format)
            artifacts.append(
                _artifact(
                    path,
                    role=role,
                    row_count=int(len(frame)),
                    output_format=output_format,
                )
            )
        report_path = _write_text(
            str(evidence["markdown"]),
            temporary_directory / "interval_health_operator_report.md",
        )
        artifacts.append(
            _artifact(
                report_path,
                role="interval_health_operator_report",
                row_count=None,
                output_format=None,
            )
        )
        contract = load_source_area_contract()
        manifest = {
            "health_demo_run_id": run_id,
            "health_demo_run_timestamp_utc": timestamp.isoformat(),
            "source_mode": "deterministic_credential_free_demo",
            "output_format": output_format,
            "source_area_contract_version": str(contract["contract_version"]),
            "source_groups": 4,
            "source_bindings": _source_bindings(history),
            **evidence["manifest"],
            "artifacts": sorted(artifacts, key=lambda item: item["artifact_role"]),
            **MANIFEST_SAFETY,
            "manifest_contract_version": MANIFEST_CONTRACT_VERSION,
        }
        manifest["manifest_hash"] = _document_hash(manifest)
        manifest_path = temporary_directory / "portfolio_interval_health_manifest.json"
        _write_text(
            json.dumps(_canonical(manifest), indent=2, sort_keys=True) + "\n",
            manifest_path,
        )
        verify_portfolio_interval_health_manifest(manifest, temporary_directory)
        temporary_directory.replace(final_directory)
        return manifest, final_directory / manifest_path.name
    except Exception:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise
