from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_g25_is_split_into_two_completed_dependency_layers():
    roadmap = text("ROADMAP.md")
    assert (
        "G25a | Reproducible interval-health trend datasets across area, "
        "horizon, model, and coverage slices | Implemented locally"
        in roadmap
    )
    assert (
        "G25b | Thin-client Markdown, HTML, and notebook reporting over retained "
        "interval-health trend datasets | Implemented locally"
        in roadmap
    )
    assert "Previous unsplit status before PRs #57–#58" in roadmap


def test_roadmap_records_repaired_g26_and_advances_to_named_decision():
    roadmap = text("ROADMAP.md")
    assert (
        "G26 | Human-reviewed interval-monitoring policy sensitivity evidence "
        "without automatic threshold, interval, model, schedule, or promotion "
        "changes | Implemented locally"
        in roadmap
    )
    assert (
        "G27 | Immutable named monitoring-policy review decision without "
        "candidate-threshold activation | Next"
        in roadmap
    )
    assert "must not activate thresholds" in roadmap
    assert "must not update the active policy" in roadmap


def test_completed_g25_and_g26_artifacts_are_present_and_non_empty():
    paths = [
        "forecasting/interval_health_trends.py",
        "forecasting/run_interval_health_trends.py",
        "INTERVAL_HEALTH_TRENDS.md",
        "forecasting/interval_health_reporting.py",
        "forecasting/run_interval_health_report.py",
        "INTERVAL_HEALTH_REPORTING.md",
        "notebooks/interval_health_trends.ipynb",
        "forecasting/interval_policy_sensitivity.py",
        "forecasting/run_interval_policy_sensitivity.py",
        "INTERVAL_POLICY_SENSITIVITY.md",
    ]
    for relative in paths:
        path = ROOT / relative
        assert path.is_file(), relative
        assert path.read_bytes().strip(), relative


def test_failed_g26_publish_markers_are_removed():
    for relative in (
        "G26_PUBLISH_MARKER.tmp",
        "NO.tmp",
        "THIS_SHOULD_NOT_EXIST.tmp",
    ):
        assert not (ROOT / relative).exists(), relative
