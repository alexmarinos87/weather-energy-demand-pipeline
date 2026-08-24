from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from forecasting.contracts import BacktestConfig
from forecasting.demand_weather_report import (
    DemandWeatherAnalysisConfig,
    build_demand_weather_analysis,
)
from forecasting.demo import build_multi_area_demo_feature_frame
from forecasting.evaluation import run_chronological_backtest
from forecasting.forecast_weather import (
    ForecastWeatherConfig,
    build_demo_forecast_weather_frame,
)
from forecasting.portfolio_seasonal import (
    SEASONAL_ARTIFACT_ROLES,
    build_portfolio_seasonal_evidence,
    verify_portfolio_seasonal_evidence,
)
from forecasting.weather_comparison import run_weather_model_comparison
from ingestion.common.source_area import load_source_area_contract


MANIFEST_CONTRACT_VERSION = "portfolio-demo-manifest-v3"
DEMO_RUN_ID_PATTERN = re.compile(r"^pdm-[0-9a-f]{24}$")
EXPECTED_SOURCE_AREAS = {
    "east_midlands",
    "south_wales",
    "south_west",
    "west_midlands",
}
BASE_ARTIFACT_ROLES = {
    "demo_features",
    "demo_source_area_summary",
    "demo_forecast_weather",
    "baseline_predictions",
    "baseline_metrics",
    "weather_comparison_predictions",
    "weather_comparison_metrics",
    "demand_weather_overview",
    "hourly_load_profile",
    "temperature_demand_profile",
    "peak_demand_events",
    "demand_weather_markdown",
}
EXPECTED_ARTIFACT_ROLES = BASE_ARTIFACT_ROLES | SEASONAL_ARTIFACT_ROLES


class PortfolioDemoError(ValueError):
    """Raised when the credential-free portfolio demo cannot be verified."""


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioDemoError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise PortfolioDemoError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_timestamp(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _document_hash(document: dict[str, Any]) -> str:
    material = {
        key: value for key, value in document.items() if key != "manifest_hash"
    }
    encoded = json.dumps(
        _canonical(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_frame(
    frame: pd.DataFrame,
    directory: Path,
    name: str,
    output_format: str,
) -> Path:
    if frame.empty:
        raise PortfolioDemoError(f"{name} must not be empty.")
    suffix = ".csv" if output_format == "csv" else ".parquet"
    path = directory / f"{name}{suffix}"
    temporary = directory / f".{name}.tmp{suffix}"
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        if output_format == "csv":
            frame.to_csv(temporary, index=False)
        else:
            frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _write_text(content: str, path: Path) -> Path:
    if not content.strip():
        raise PortfolioDemoError(f"{path.name} must not be empty.")
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


def _artifact(
    path: Path,
    *,
    role: str,
    row_count: int | None,
    output_format: str | None,
) -> dict[str, Any]:
    if row_count is not None and row_count < 1:
        raise PortfolioDemoError(f"Artifact {role} must contain at least one row.")
    content_type = (
        "text/markdown"
        if output_format is None
        else "text/csv"
        if output_format == "csv"
        else "application/x-parquet"
    )
    return {
        "artifact_role": role,
        "relative_path": path.name,
        "content_type": content_type,
        "size_bytes": path.stat().st_size,
        "sha256": _file_hash(path),
        "row_count": row_count,
    }


def _read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise PortfolioDemoError(f"Cannot read tabular artifact {path.name}.")


def _read_row_count(path: Path) -> int:
    return int(len(_read_frame(path)))


def _binding_tuple(binding: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(binding.get("source_area", "")),
        str(binding.get("resource_id", "")),
        str(binding.get("city", "")),
    )


def _validate_source_bindings(manifest: dict[str, Any]) -> list[dict[str, str]]:
    contract_version = manifest.get("source_area_contract_version")
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise PortfolioDemoError(
            "Portfolio demo source-area contract version is missing."
        )
    bindings = manifest.get("source_bindings")
    if not isinstance(bindings, list) or len(bindings) != 4:
        raise PortfolioDemoError(
            "Portfolio demo must contain four source-area bindings."
        )
    normalized: list[dict[str, str]] = []
    for binding in bindings:
        if not isinstance(binding, dict):
            raise PortfolioDemoError("Portfolio demo source binding must be an object.")
        source_area, resource_id, city = _binding_tuple(binding)
        if not source_area or not resource_id or not city:
            raise PortfolioDemoError(
                "Portfolio demo source bindings require area, resource, and city."
            )
        normalized.append(
            {
                "source_area": source_area,
                "resource_id": resource_id,
                "city": city,
            }
        )
    tuples = [_binding_tuple(binding) for binding in normalized]
    if len(set(tuples)) != len(tuples):
        raise PortfolioDemoError("Portfolio demo source bindings are duplicated.")
    if {binding["source_area"] for binding in normalized} != EXPECTED_SOURCE_AREAS:
        raise PortfolioDemoError(
            "Portfolio demo source bindings do not cover all contracted areas."
        )
    if manifest.get("source_groups") != len(normalized):
        raise PortfolioDemoError(
            "Portfolio demo source group count does not match its bindings."
        )
    return sorted(normalized, key=_binding_tuple)


def verify_portfolio_demo_manifest(
    manifest: dict[str, Any],
    run_directory: Path,
) -> None:
    """Verify immutable artifacts, spatial isolation, and no-side-effect flags."""
    if not isinstance(manifest, dict):
        raise PortfolioDemoError("Portfolio demo manifest must be a JSON object.")
    run_id = str(manifest.get("demo_run_id", ""))
    if not DEMO_RUN_ID_PATTERN.fullmatch(run_id):
        raise PortfolioDemoError("Portfolio demo run ID is malformed.")
    _utc_timestamp(
        manifest.get("demo_run_timestamp_utc"), "demo_run_timestamp_utc"
    )
    if manifest.get("manifest_contract_version") != MANIFEST_CONTRACT_VERSION:
        raise PortfolioDemoError("Unsupported portfolio demo manifest contract.")
    if manifest.get("manifest_hash") != _document_hash(manifest):
        raise PortfolioDemoError("Portfolio demo manifest hash is invalid.")
    if manifest.get("source_mode") != "deterministic_credential_free_demo":
        raise PortfolioDemoError("Portfolio demo source mode is invalid.")
    output_format = manifest.get("output_format")
    if output_format not in {"csv", "parquet"}:
        raise PortfolioDemoError("Portfolio demo output format is invalid.")
    bindings = _validate_source_bindings(manifest)
    safety = {
        "credential_free": True,
        "live_source_calls_performed": False,
        "fabric_operations_performed": False,
        "schedule_activation_performed": False,
        "model_promotion_performed": False,
        "external_publication_performed": False,
    }
    for field, expected in safety.items():
        if manifest.get(field) is not expected:
            raise PortfolioDemoError(f"Portfolio demo safety flag {field} is invalid.")
    horizons = manifest.get("demand_horizons_minutes")
    if horizons != [30, 60]:
        raise PortfolioDemoError("Portfolio demo must expose 30- and 60-minute horizons.")
    if set(manifest.get("baseline_models", ())) != {
        "persistence_current_value",
        "ridge_weather_lag",
    }:
        raise PortfolioDemoError("Portfolio demo baseline model identities are invalid.")
    if set(manifest.get("comparison_models", ())) != {
        "ridge_weather_lag",
        "ridge_target_weather",
    }:
        raise PortfolioDemoError("Portfolio demo comparison model identities are invalid.")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise PortfolioDemoError("Portfolio demo artifacts must be a list.")
    roles = [artifact.get("artifact_role") for artifact in artifacts]
    if set(roles) != EXPECTED_ARTIFACT_ROLES or len(roles) != len(set(roles)):
        raise PortfolioDemoError("Portfolio demo artifact roles are incomplete or duplicated.")
    run_directory = Path(run_directory)
    by_role: dict[str, Path] = {}
    for artifact in artifacts:
        relative = Path(str(artifact.get("relative_path", "")))
        if relative.is_absolute() or len(relative.parts) != 1:
            raise PortfolioDemoError("Portfolio demo artifact paths must be local filenames.")
        path = run_directory / relative
        if not path.is_file() or path.is_symlink():
            raise PortfolioDemoError(f"Portfolio demo artifact is missing: {relative}.")
        if path.stat().st_size != int(artifact.get("size_bytes", -1)):
            raise PortfolioDemoError(f"Portfolio demo artifact size changed: {relative}.")
        if _file_hash(path) != artifact.get("sha256"):
            raise PortfolioDemoError(f"Portfolio demo artifact hash changed: {relative}.")
        row_count = artifact.get("row_count")
        if row_count is None:
            if path.suffix.lower() != ".md":
                raise PortfolioDemoError(
                    f"Only Markdown artifacts may omit row count: {relative}."
                )
        elif _read_row_count(path) != int(row_count):
            raise PortfolioDemoError(
                f"Portfolio demo artifact row count changed: {relative}."
            )
        by_role[str(artifact["artifact_role"])] = path

    features = _read_frame(by_role["demo_features"])
    required = {"source_area", "resource_id", "city", "event_timestamp_utc"}
    if not required.issubset(features.columns):
        raise PortfolioDemoError("Portfolio demo features lack spatial identity.")
    feature_bindings = sorted(
        [
            {
                "source_area": str(row.source_area),
                "resource_id": str(row.resource_id),
                "city": str(row.city),
            }
            for row in features[
                ["source_area", "resource_id", "city"]
            ].drop_duplicates().itertuples(index=False)
        ],
        key=_binding_tuple,
    )
    if feature_bindings != bindings:
        raise PortfolioDemoError(
            "Portfolio demo feature groups do not match manifest source bindings."
        )
    if features.duplicated(
        subset=["source_area", "resource_id", "city", "event_timestamp_utc"],
        keep=False,
    ).any():
        raise PortfolioDemoError(
            "Portfolio demo contains duplicate group/timestamp identities."
        )
    summary = _read_frame(by_role["demo_source_area_summary"])
    if set(summary["source_area"].astype(str)) != EXPECTED_SOURCE_AREAS:
        raise PortfolioDemoError(
            "Portfolio demo source-area summary is incomplete."
        )
    if len(summary) != 4 or (summary["observation_count"] < 1).any():
        raise PortfolioDemoError(
            "Portfolio demo source-area summary counts are invalid."
        )

    seasonal_frames = {
        role: _read_frame(by_role[role])
        for role in SEASONAL_ARTIFACT_ROLES
        if role != "model_family_summary_markdown"
    }
    try:
        verify_portfolio_seasonal_evidence(
            manifest=manifest,
            frames_by_role=seasonal_frames,
            expected_source_areas=EXPECTED_SOURCE_AREAS,
        )
    except ValueError as exc:
        raise PortfolioDemoError(str(exc)) from exc
    seasonal_report = by_role["model_family_summary_markdown"].read_text(
        encoding="utf-8"
    )
    if "comparison evidence only" not in seasonal_report:
        raise PortfolioDemoError(
            "Portfolio model-family summary lacks its review-only boundary."
        )


def _source_area_summary(features: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for identity, group in features.groupby(
        ["source_area", "resource_id", "city"], sort=True, dropna=False
    ):
        source_area, resource_id, city = identity
        rows.append(
            {
                "source_area": source_area,
                "resource_id": resource_id,
                "city": city,
                "observation_count": int(len(group)),
                "observation_start_utc": group["event_timestamp_utc"].min(),
                "observation_end_utc": group["event_timestamp_utc"].max(),
                "demand_mean_mw": float(group["demand_mw"].mean()),
                "demand_min_mw": float(group["demand_mw"].min()),
                "demand_max_mw": float(group["demand_mw"].max()),
                "temperature_mean_c": float(group["temperature"].mean()),
                "humidity_mean_pct": float(group["humidity"].mean()),
            }
        )
    summary = pd.DataFrame(rows)
    if len(summary) != 4 or set(summary["source_area"]) != EXPECTED_SOURCE_AREAS:
        raise PortfolioDemoError(
            "The multi-area demo must summarize exactly four contracted areas."
        )
    return summary


def run_portfolio_demo(
    output_root: Path,
    *,
    output_format: str = "csv",
    run_id: str | None = None,
    run_timestamp: Any | None = None,
) -> tuple[dict[str, Any], Path]:
    """Run the complete deterministic multi-area product journey atomically."""
    if output_format not in {"csv", "parquet"}:
        raise PortfolioDemoError("output_format must be csv or parquet.")
    run_id = run_id or "pdm-" + uuid4().hex[:24]
    if not DEMO_RUN_ID_PATTERN.fullmatch(run_id):
        raise PortfolioDemoError("run_id must match pdm- plus 24 lowercase hex characters.")
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
        contract = load_source_area_contract()
        features = build_multi_area_demo_feature_frame()
        source_summary = _source_area_summary(features)
        source_bindings = [
            {
                "source_area": str(row.source_area),
                "resource_id": str(row.resource_id),
                "city": str(row.city),
            }
            for row in source_summary[
                ["source_area", "resource_id", "city"]
            ].sort_values(["source_area", "resource_id", "city"]).itertuples(index=False)
        ]
        forecast_weather = build_demo_forecast_weather_frame(
            features, horizon_minutes=(30, 60)
        )
        backtest_config = BacktestConfig(
            horizon_minutes=(30, 60),
            target_tolerance_minutes=5,
            min_target_coverage=0.90,
        )
        forecast_config = ForecastWeatherConfig(
            valid_time_tolerance_minutes=0,
            max_availability_age_minutes=180,
            min_coverage=1.0,
        )
        baseline_predictions, baseline_metrics = run_chronological_backtest(
            features,
            config=backtest_config,
            run_id=f"{run_id}-baseline",
            run_timestamp=timestamp.to_pydatetime(),
        )
        comparison_predictions, comparison_metrics = run_weather_model_comparison(
            features,
            forecast_weather,
            backtest_config=backtest_config,
            forecast_config=forecast_config,
            evaluation_mode="holdout",
            run_id=f"{run_id}-comparison",
            run_timestamp=timestamp.to_pydatetime(),
        )
        seasonal = build_portfolio_seasonal_evidence(
            source_areas=tuple(sorted(EXPECTED_SOURCE_AREAS)),
            run_id=run_id,
            run_timestamp=timestamp.to_pydatetime(),
        )
        analytics = build_demand_weather_analysis(
            features,
            config=DemandWeatherAnalysisConfig(
                top_peak_count=10,
                temperature_bin_width_c=2.5,
            ),
            run_id="dwa-" + run_id.split("-", 1)[1],
            run_timestamp=timestamp,
        )

        frame_outputs = [
            ("demo_features", "demo_features", features),
            (
                "demo_source_area_summary",
                "demo_source_area_summary",
                source_summary,
            ),
            ("demo_forecast_weather", "demo_forecast_weather", forecast_weather),
            ("baseline_predictions", "baseline_predictions", baseline_predictions),
            ("baseline_metrics", "baseline_metrics", baseline_metrics),
            (
                "weather_comparison_predictions",
                "weather_comparison_predictions",
                comparison_predictions,
            ),
            (
                "weather_comparison_metrics",
                "weather_comparison_metrics",
                comparison_metrics,
            ),
            (
                "demand_weather_overview",
                "demand_weather_overview",
                analytics["overview"],
            ),
            (
                "hourly_load_profile",
                "hourly_load_profile",
                analytics["hourly_load_profile"],
            ),
            (
                "temperature_demand_profile",
                "temperature_demand_profile",
                analytics["temperature_demand_profile"],
            ),
            (
                "peak_demand_events",
                "peak_demand_events",
                analytics["peak_demand_events"],
            ),
            *[
                (role, role, frame)
                for role, frame in seasonal["frames"].items()
            ],
        ]
        artifacts: list[dict[str, Any]] = []
        for role, name, frame in frame_outputs:
            path = _write_frame(
                frame, temporary_directory, name, output_format
            )
            artifacts.append(
                _artifact(
                    path,
                    role=role,
                    row_count=int(len(frame)),
                    output_format=output_format,
                )
            )
        report_path = _write_text(
            str(analytics["markdown"]),
            temporary_directory / "demand_weather_report.md",
        )
        artifacts.append(
            _artifact(
                report_path,
                role="demand_weather_markdown",
                row_count=None,
                output_format=None,
            )
        )
        seasonal_report_path = _write_text(
            str(seasonal["markdown"]),
            temporary_directory / "model_family_summary.md",
        )
        artifacts.append(
            _artifact(
                seasonal_report_path,
                role="model_family_summary_markdown",
                row_count=None,
                output_format=None,
            )
        )
        manifest = {
            "demo_run_id": run_id,
            "demo_run_timestamp_utc": timestamp.isoformat(),
            "source_mode": "deterministic_credential_free_demo",
            "output_format": output_format,
            "source_area_contract_version": str(contract["contract_version"]),
            "source_groups": len(source_bindings),
            "source_bindings": source_bindings,
            "demand_horizons_minutes": sorted(
                baseline_predictions["requested_horizon_minutes"]
                .astype(int)
                .unique()
                .tolist()
            ),
            "baseline_models": sorted(
                baseline_predictions["model_name"].unique().tolist()
            ),
            "comparison_models": sorted(
                comparison_predictions["model_name"].unique().tolist()
            ),
            **seasonal["manifest"],
            "artifacts": sorted(
                artifacts, key=lambda artifact: artifact["artifact_role"]
            ),
            "credential_free": True,
            "live_source_calls_performed": False,
            "fabric_operations_performed": False,
            "schedule_activation_performed": False,
            "model_promotion_performed": False,
            "external_publication_performed": False,
            "manifest_contract_version": MANIFEST_CONTRACT_VERSION,
        }
        manifest["manifest_hash"] = _document_hash(manifest)
        manifest_path = temporary_directory / "portfolio_demo_manifest.json"
        _write_text(
            json.dumps(_canonical(manifest), indent=2, sort_keys=True) + "\n",
            manifest_path,
        )
        verify_portfolio_demo_manifest(manifest, temporary_directory)
        temporary_directory.replace(final_directory)
        return manifest, final_directory / manifest_path.name
    except Exception:
        shutil.rmtree(temporary_directory, ignore_errors=True)
        raise
