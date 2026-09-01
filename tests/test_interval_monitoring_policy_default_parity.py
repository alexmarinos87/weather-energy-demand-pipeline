from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL_PATH = ROOT / "forecasting" / "interval_monitoring.py"
FABRIC_PATH = ROOT / "fabric" / "notebooks" / "05f_prediction_interval_monitoring.py"
DOCS_PATH = ROOT / "INTERVAL_MONITORING.md"


def _class_default(source: str, class_name: str, field_name: str):
    tree = ast.parse(source)
    class_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    assignment = next(
        node
        for node in class_node.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == field_name
    )
    return ast.literal_eval(assignment.value)


def _module_constant(source: str, name: str):
    tree = ast.parse(source)
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name
    )
    return ast.literal_eval(assignment.value)


def test_reviewed_recent_coverage_shortfall_default_has_local_fabric_parity():
    local_source = LOCAL_PATH.read_text(encoding="utf-8")
    fabric_source = FABRIC_PATH.read_text(encoding="utf-8")

    local_value = _class_default(
        local_source,
        "PredictionIntervalMonitoringConfig",
        "max_recent_coverage_shortfall_pct_points",
    )
    fabric_value = _module_constant(
        fabric_source,
        "MAX_RECENT_COVERAGE_SHORTFALL_PCT_POINTS",
    )

    assert local_value == fabric_value == 3.0


def test_monitoring_documentation_records_the_reviewed_three_point_limit():
    documentation = DOCS_PATH.read_text(encoding="utf-8")
    assert "recent empirical coverage shortfall <= 3 percentage points" in documentation
    assert "narrows this last hard limit from five\nto three percentage points" in documentation
    assert "recent empirical coverage shortfall <= 5 percentage points" not in documentation
