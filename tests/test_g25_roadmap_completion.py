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
    assert (
        "Previous unsplit status before PRs #57–#58" in roadmap
    )
    assert (
        "The historical line above is retained only to make the roadmap "
        "transition" in roadmap
    )


def test_roadmap_advances_to_human_reviewed_policy_sensitivity():
    roadmap = text("ROADMAP.md")
    assert (
        "G26 | Human-reviewed interval-monitoring policy sensitivity evidence "
        "without automatic threshold, interval, model, schedule, or promotion "
        "changes | Next"
        in roadmap
    )
    assert "must not update the active policy" in roadmap
    assert "must not rerun monitoring logic" in roadmap


def test_completed_g25_artifacts_are_present_and_non_empty():
    paths = [
        "forecasting/interval_health_trends.py",
        "forecasting/run_interval_health_trends.py",
        "INTERVAL_HEALTH_TRENDS.md",
        "forecasting/interval_health_reporting.py",
        "forecasting/run_interval_health_report.py",
        "INTERVAL_HEALTH_REPORTING.md",
        "notebooks/interval_health_trends.ipynb",
    ]
    for relative in paths:
        path = ROOT / relative
        assert path.is_file(), relative
        assert path.read_bytes().strip(), relative
