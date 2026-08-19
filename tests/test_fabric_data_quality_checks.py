import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = PROJECT_ROOT / "fabric" / "notebooks" / "04_data_quality_checks.py"


def _load_notebook_namespace() -> dict:
    return runpy.run_path(str(NOTEBOOK_PATH), run_name="fabric_data_quality_notebook")


def test_fabric_data_quality_includes_freshness_and_spatial_checks():
    namespace = _load_notebook_namespace()
    checks = namespace["build_checks"](max_expected_data_lag_hours=4)
    checks_by_name = {check["check_name"]: check for check in checks}

    assert checks_by_name["silver_weather_freshness"]["severity"] == "warn"
    assert checks_by_name["gold_weather_cross_area_match"]["severity"] == "error"
    assert checks_by_name["gold_weather_future_match"]["severity"] == "error"
    assert checks_by_name["silver_energy_unscoped_source_area"]["severity"] == "warn"
    assert "INTERVAL 4 HOURS" in checks_by_name["gold_feature_freshness"]["sql"]


def test_fabric_data_quality_rejects_invalid_freshness_threshold():
    namespace = _load_notebook_namespace()
    with pytest.raises(ValueError, match="at least 1"):
        namespace["build_checks"](max_expected_data_lag_hours=0)
