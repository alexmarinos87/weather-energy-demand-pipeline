from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from forecasting.demand_weather_report import (
    ANALYSIS_CONTRACT_VERSION,
    DemandWeatherAnalysisConfig,
    DemandWeatherAnalysisError,
    build_demand_weather_analysis,
    prepare_demand_weather_features,
)
from forecasting.demo import build_demo_feature_frame
from forecasting.run_demand_weather_report import main


ROOT = Path(__file__).resolve().parents[1]


def _analysis(frame=None, **config_kwargs):
    return build_demand_weather_analysis(
        frame if frame is not None else build_demo_feature_frame(),
        config=DemandWeatherAnalysisConfig(**config_kwargs),
        run_id="dwa-" + "1" * 24,
        run_timestamp=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )


def test_demo_analysis_produces_all_report_surfaces():
    analysis = _analysis()
    assert set(analysis) == {
        "prepared_features",
        "overview",
        "hourly_load_profile",
        "temperature_demand_profile",
        "peak_demand_events",
        "markdown",
    }
    assert len(analysis["overview"]) == 1
    assert len(analysis["hourly_load_profile"]) == 24
    assert not analysis["temperature_demand_profile"].empty
    assert len(analysis["peak_demand_events"]) == 10
    assert analysis["overview"].loc[0, "analysis_contract_version"] == (
        ANALYSIS_CONTRACT_VERSION
    )


def test_overview_statistics_match_prepared_source_evidence():
    frame = build_demo_feature_frame()
    analysis = _analysis(frame)
    overview = analysis["overview"].iloc[0]
    assert overview["observation_count"] == len(frame)
    assert overview["demand_mean_mw"] == pytest.approx(frame["demand_mw"].mean())
    assert overview["demand_max_mw"] == pytest.approx(frame["demand_mw"].max())
    assert overview["temperature_min_c"] == pytest.approx(
        frame["temperature"].min()
    )
    assert overview["temperature_max_c"] == pytest.approx(
        frame["temperature"].max()
    )
    assert overview["median_interval_minutes"] == 5.0


def test_temperature_profile_uses_fixed_width_non_overlapping_bands():
    profile = _analysis(temperature_bin_width_c=2.5)[
        "temperature_demand_profile"
    ]
    assert (
        profile["temperature_bin_upper_c"]
        - profile["temperature_bin_lower_c"]
    ).eq(2.5).all()
    assert profile["temperature_bin_lower_c"].is_monotonic_increasing
    assert profile["observation_count"].sum() == len(build_demo_feature_frame())


def test_peak_events_are_ranked_per_group_and_preserve_weather_evidence():
    peaks = _analysis(top_peak_count=5)["peak_demand_events"]
    assert peaks["peak_rank"].tolist() == [1, 2, 3, 4, 5]
    assert peaks["demand_mw"].is_monotonic_decreasing
    assert peaks[
        ["event_timestamp_utc", "temperature_c", "humidity_pct", "weather_age_minutes"]
    ].notna().all().all()


def test_multiple_source_groups_are_never_aggregated_together():
    first = build_demo_feature_frame(periods=120)
    second = first.copy()
    second["source_area"] = "south_west"
    second["resource_id"] = "resource-2"
    second["city"] = "Bristol"
    second["demand_mw"] = second["demand_mw"] + 1000.0
    analysis = _analysis(pd.concat([first, second], ignore_index=True), top_peak_count=3)
    assert len(analysis["overview"]) == 2
    means = analysis["overview"].set_index("source_area")["demand_mean_mw"]
    assert means["south_west"] - means["east_midlands"] == pytest.approx(1000.0)
    assert analysis["peak_demand_events"].groupby("source_area").size().to_dict() == {
        "east_midlands": 3,
        "south_west": 3,
    }


def test_naive_timestamp_is_rejected():
    frame = build_demo_feature_frame()
    frame["event_timestamp_utc"] = frame["event_timestamp_utc"].astype(object)
    frame.loc[0, "event_timestamp_utc"] = "2026-01-01T00:05:00"
    with pytest.raises(DemandWeatherAnalysisError, match="timezone-aware"):
        prepare_demand_weather_features(frame)


def test_duplicate_group_timestamp_is_rejected():
    frame = build_demo_feature_frame()
    frame.loc[1, "event_timestamp_utc"] = frame.loc[0, "event_timestamp_utc"]
    with pytest.raises(DemandWeatherAnalysisError, match="duplicate"):
        prepare_demand_weather_features(frame)


def test_invalid_humidity_and_weather_age_are_rejected():
    frame = build_demo_feature_frame()
    frame.loc[0, "humidity"] = 101.0
    with pytest.raises(DemandWeatherAnalysisError, match="between 0 and 100"):
        prepare_demand_weather_features(frame)

    frame = build_demo_feature_frame()
    frame.loc[0, "weather_age_minutes"] = -1.0
    with pytest.raises(DemandWeatherAnalysisError, match="non-negative"):
        prepare_demand_weather_features(frame)


def test_group_requires_at_least_two_observations():
    frame = build_demo_feature_frame().iloc[[0]].copy()
    with pytest.raises(DemandWeatherAnalysisError, match="at least two"):
        prepare_demand_weather_features(frame)


def test_constant_weather_correlation_is_reported_as_not_estimable():
    frame = build_demo_feature_frame(periods=120)
    frame["temperature"] = 10.0
    frame["humidity"] = 60.0
    overview = _analysis(frame)["overview"].iloc[0]
    assert pd.isna(overview["demand_temperature_pearson"])
    assert pd.isna(overview["demand_humidity_pearson"])


def test_markdown_states_descriptive_and_proxy_boundaries():
    report = _analysis()["markdown"]
    assert "This report is descriptive" in report
    assert "Correlation does not establish causation" in report
    assert "representative project weather proxy" in report
    assert "Highest-demand observations" in report


def test_overview_satisfies_versioned_json_schema():
    overview = _analysis()["overview"].iloc[0].to_dict()
    for column in (
        "analysis_timestamp_utc",
        "observation_start_utc",
        "observation_end_utc",
    ):
        overview[column] = pd.Timestamp(overview[column]).isoformat()
    for column, value in list(overview.items()):
        if pd.isna(value):
            overview[column] = None
    schema = json.loads(
        (
            ROOT
            / "data-contracts"
            / "demand_weather_analysis_summary_schema.json"
        ).read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).iter_errors(overview)
    )
    assert errors == []


def test_cli_writes_immutable_credential_free_demo_outputs(tmp_path):
    output = tmp_path / "report"
    assert (
        main(
            [
                "--demo",
                "--top-peak-count",
                "4",
                "--temperature-bin-width-c",
                "2.5",
                "--output-dir",
                str(output),
                "--output-format",
                "csv",
            ]
        )
        == 0
    )
    assert len(list(output.glob("demand_weather_overview_*.csv"))) == 1
    assert len(list(output.glob("hourly_load_profile_*.csv"))) == 1
    assert len(list(output.glob("temperature_demand_profile_*.csv"))) == 1
    peak_files = list(output.glob("peak_demand_events_*.csv"))
    assert len(peak_files) == 1
    assert len(pd.read_csv(peak_files[0])) == 4
    markdown_files = list(output.glob("demand_weather_report_*.md"))
    assert len(markdown_files) == 1
    assert "This report is descriptive" in markdown_files[0].read_text(
        encoding="utf-8"
    )


def test_report_configuration_rejects_invalid_bounds():
    with pytest.raises(DemandWeatherAnalysisError, match="positive integer"):
        DemandWeatherAnalysisConfig(top_peak_count=0).validate()
    with pytest.raises(DemandWeatherAnalysisError, match="finite and positive"):
        DemandWeatherAnalysisConfig(temperature_bin_width_c=0).validate()


@pytest.mark.parametrize(
    "notebook_path",
    [
        "notebooks/exploratory_analysis.ipynb",
        "notebooks/demand_vs_temperature.ipynb",
    ],
)
def test_notebooks_are_valid_nonempty_thin_clients(notebook_path):
    path = ROOT / notebook_path
    assert path.stat().st_size > 0
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    assert notebook["cells"]
    code = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )
    assert "build_demand_weather_analysis" in code
    assert "requests" not in code
    assert "OPENWEATHER_API_KEY" not in code
    for cell in notebook["cells"]:
        if cell.get("cell_type") == "code":
            assert cell.get("outputs") == []
            assert cell.get("execution_count") is None
