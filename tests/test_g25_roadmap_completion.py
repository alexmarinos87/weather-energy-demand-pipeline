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


def test_roadmap_records_g26_through_g34_and_advances_to_g35():
    roadmap = text("ROADMAP.md")
    assert (
        "G26 | Human-reviewed interval-monitoring policy sensitivity evidence "
        "without automatic threshold, interval, model, schedule, or promotion "
        "changes | Implemented locally"
        in roadmap
    )
    assert (
        "G27 | Immutable named monitoring-policy review decision without "
        "candidate-threshold activation | Implemented locally"
        in roadmap
    )
    assert (
        "G28 | Append-only verified policy-decision ledger with duplicate and "
        "conflict detection | Implemented locally"
        in roadmap
    )
    assert (
        "G29 | Human-authored candidate-revision package bound to a G27 revision "
        "request without threshold activation | Implemented locally"
        in roadmap
    )
    assert (
        "G30 | Immutable named review of one candidate-revision package without "
        "sensitivity execution or threshold activation | Implemented locally"
        in roadmap
    )
    assert (
        "G31 | Evidence-only revised-candidate sensitivity comparison bound to "
        "an accepted G30 review | Implemented locally"
        in roadmap
    )
    assert (
        "G32 | Immutable named human disposition over one G31 result without "
        "active-policy mutation | Implemented locally"
        in roadmap
    )
    assert (
        "G33 | Repository-base-bound dry-run implementation proposal over a "
        "suitable G32 disposition without code application | Implemented locally"
        in roadmap
    )
    assert (
        "G34 | Immutable named review of one G33 dry run without code-change "
        "authorization or creation | Implemented locally"
        in roadmap
    )
    assert (
        "G35 | Separate reviewed code-change PR applying one accepted G34 dry "
        "run after exact-base revalidation, with no runtime activation | Next"
        in roadmap
    )
    assert "reject duplicate IDs and" in roadmap
    assert "address every" in roadmap
    assert "reuse the canonical evaluator" in roadmap
    assert "current repository base" in roadmap
    assert "cannot apply or authorize the change" in roadmap
    assert "cannot create, authorize, or merge that PR" in roadmap
    assert "must revalidate every G33/G34 base" in roadmap


def test_completed_g25_through_g34_artifacts_are_present_and_non_empty():
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
        "forecasting/interval_policy_review_decision.py",
        "forecasting/run_interval_policy_review_decision.py",
        "INTERVAL_POLICY_REVIEW_DECISION.md",
        "forecasting/interval_policy_decision_ledger.py",
        "forecasting/run_interval_policy_decision_ledger.py",
        "INTERVAL_POLICY_DECISION_LEDGER.md",
        "forecasting/interval_policy_candidate_revision.py",
        "forecasting/run_interval_policy_candidate_revision.py",
        "INTERVAL_POLICY_CANDIDATE_REVISION.md",
        "forecasting/interval_policy_candidate_revision_review.py",
        "forecasting/run_interval_policy_candidate_revision_review.py",
        "INTERVAL_POLICY_CANDIDATE_REVISION_REVIEW.md",
        "forecasting/interval_policy_revision_sensitivity.py",
        "forecasting/run_interval_policy_revision_sensitivity.py",
        "INTERVAL_POLICY_REVISION_SENSITIVITY.md",
        "forecasting/interval_policy_revision_disposition.py",
        "forecasting/run_interval_policy_revision_disposition.py",
        "data-contracts/interval_policy_revision_disposition_schema.json",
        "INTERVAL_POLICY_REVISION_DISPOSITION.md",
        "forecasting/interval_policy_implementation_dry_run.py",
        "forecasting/run_interval_policy_implementation_dry_run.py",
        "data-contracts/interval_policy_implementation_dry_run_schema.json",
        "INTERVAL_POLICY_IMPLEMENTATION_DRY_RUN.md",
        "forecasting/interval_policy_implementation_dry_run_review.py",
        "forecasting/run_interval_policy_implementation_dry_run_review.py",
        "data-contracts/interval_policy_implementation_dry_run_review_schema.json",
        "INTERVAL_POLICY_IMPLEMENTATION_DRY_RUN_REVIEW.md",
    ]
    for relative in paths:
        path = ROOT / relative
        assert path.is_file(), relative
        assert path.read_bytes().strip(), relative


def test_failed_publish_and_probe_artifacts_are_removed():
    for relative in (
        "G26_PUBLISH_MARKER.tmp",
        "NO.tmp",
        "THIS_SHOULD_NOT_EXIST.tmp",
        "__probe_do_not_keep__",
        "__probe_should_not_exist_2__",
    ):
        assert not (ROOT / relative).exists(), relative
