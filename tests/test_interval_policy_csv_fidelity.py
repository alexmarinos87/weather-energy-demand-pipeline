"""Input-fidelity regressions through both real compatibility commands."""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from interval_policy_evidence_fixtures import three_scenario_evidence
from forecasting import run_interval_policy_compatibility as direct
from forecasting import run_interval_policy_retained_compatibility as scenario
from forecasting.interval_policy_compatibility import assess_retained_policy_compatibility

TIME = "2026-01-20T01:00:00Z"


@pytest.fixture
def evidence():
    _, checks, trends = three_scenario_evidence()
    return (
        checks.loc[checks["monitor_run_id"].eq("monitor-tightened")].copy(),
        trends.loc[trends["scenario"].eq("tightened")].copy(),
    )


def invoke(kind, root, checks, trends, input_format="csv"):
    root.mkdir(parents=True, exist_ok=True)
    checks_path = root / f"checks.{input_format}"
    trends_path = root / f"trends.{input_format}"
    for frame, path in ((checks, checks_path), (trends, trends_path)):
        if input_format == "csv":
            frame.to_csv(path, index=False)
        else:
            frame.to_parquet(path, index=False)
    originals = {path: path.read_bytes() for path in (checks_path, trends_path)}
    args, main = arguments(kind, root)
    if input_format != "csv":
        args = [value.replace("checks.csv", checks_path.name).replace("trends.csv", trends_path.name)
                for value in args]
    assert main(args) == 0
    assert {path: path.read_bytes() for path in originals} == originals
    output = root / "output"
    slices = pd.read_csv(next(output.glob("*_slices_*.csv")), dtype=str, keep_default_na=False)
    summary = pd.read_csv(next(output.glob("*_summary_*.csv")), dtype=str, keep_default_na=False)
    return slices, summary


def arguments(kind, root):
    args = ["--health-checks", str(root / "checks.csv"),
            "--output-dir", str(root / "output"), "--output-format", "csv"]
    if kind == "direct":
        return args + ["--assessment-run-id", "ipc-" + "1" * 24,
                       "--assessment-timestamp-utc", TIME], direct.main
    return args + ["--slice-trends", str(root / "trends.csv"),
                   "--compatibility-run-id", "ipca-" + "1" * 24,
                   "--compatibility-run-timestamp", TIME], scenario.main


@pytest.mark.parametrize("kind", ["direct", "scenario"])
@pytest.mark.parametrize("column,value", [
    ("resource_id", "0012"), ("resource_id", "NA"),
    ("resource_id", "NULL"), ("resource_id", "1e3"),
    ("monitor_run_id", "0007"), ("latest_interval_run_id", "0042"),
    ("city", 'Harbour, "GB"\nDistrict'),
])
def test_csv_and_parquet_preserve_identical_source_identities(evidence, tmp_path, kind, column, value):
    checks, trends = evidence
    checks[column] = value
    trends[column] = value
    expected_slices, expected_summary = assess_retained_policy_compatibility(
        checks, assessment_run_id="ipc-" + "1" * 24, assessment_timestamp_utc=TIME,
    )
    results = [invoke(kind, tmp_path / format_, checks, trends, format_)
               for format_ in ("csv", "parquet")]
    for slices, summary in results:
        assert set(summary["source_health_checks_sha256"]) == set(expected_summary["source_health_checks_sha256"])
        for identity in ("resource_id", "city", "source_monitor_run_id"):
            assert set(slices[identity]) == set(expected_slices[identity])
    pd.testing.assert_frame_equal(results[0][0], results[1][0])
    pd.testing.assert_frame_equal(results[0][1], results[1][1])


@pytest.mark.parametrize("kind,target", [("direct", "checks"), ("scenario", "checks"), ("scenario", "trends")])
@pytest.mark.parametrize("header", ["resource_id", ""])
def test_ambiguous_headers_are_rejected_before_output(evidence, tmp_path, kind, target, header):
    checks, trends = evidence
    checks.to_csv(tmp_path / "checks.csv", index=False)
    trends.to_csv(tmp_path / "trends.csv", index=False)
    path = tmp_path / f"{target}.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        records = list(csv.reader(handle))
    records[0].append(header)
    for row in records[1:]:
        row.append("conflicting-extra-field")
    with path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(records)
    original = path.read_bytes()
    args, main = arguments(kind, tmp_path)
    with pytest.raises(ValueError, match="header"):
        main(args)
    assert not (tmp_path / "output").exists()
    assert path.read_bytes() == original


@pytest.mark.parametrize("kind", ["direct", "scenario"])
@pytest.mark.parametrize("column,value", [("resource_id", ""), ("observed_value", "NA")])
def test_preserving_text_does_not_accept_blank_ids_or_invalid_numbers(evidence, tmp_path, kind, column, value):
    checks, trends = evidence
    checks[column] = value
    if column in trends:
        trends[column] = value
    checks.to_csv(tmp_path / "checks.csv", index=False)
    trends.to_csv(tmp_path / "trends.csv", index=False)
    args, main = arguments(kind, tmp_path)
    with pytest.raises(ValueError):
        main(args)
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("kind", ["direct", "scenario"])
def test_utf8_bom_and_blank_lines_are_supported(evidence, tmp_path, kind):
    checks, trends = evidence
    for frame, name in ((checks, "checks"), (trends, "trends")):
        content = frame.to_csv(index=False)
        (tmp_path / f"{name}.csv").write_text("\ufeff\n" + content + "\n", encoding="utf-8")
    args, main = arguments(kind, tmp_path)
    assert main(args) == 0
