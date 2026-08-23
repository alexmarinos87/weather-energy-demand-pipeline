from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
APPROVED_ACTIONS = {
    "actions/checkout": "de0fac2e4500dabe0009e67214ff5f5447ce83dd",
    "actions/setup-python": "a309ff8b426b58ec0e2a45f0f869d46889d02405",
}
USES_PATTERN = re.compile(
    r"^\s*uses:\s*([^@\s]+)@([0-9a-f]{40})(?:\s+#\s*(\S+))?\s*$",
    re.MULTILINE,
)


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_every_external_action_is_commit_pinned_and_allowlisted():
    text = workflow_text()
    uses_lines = [line for line in text.splitlines() if "uses:" in line]
    matches = USES_PATTERN.findall(text)
    assert len(matches) == len(uses_lines) == len(APPROVED_ACTIONS)
    observed = {action: commit for action, commit, _ in matches}
    assert observed == APPROVED_ACTIONS
    assert "@v" not in text
    assert "@main" not in text
    assert "@master" not in text


def test_reviewed_release_comments_match_the_approved_action_commits():
    matches = {
        action: (commit, version)
        for action, commit, version in USES_PATTERN.findall(workflow_text())
    }
    assert matches["actions/checkout"] == (
        APPROVED_ACTIONS["actions/checkout"],
        "v6.0.2",
    )
    assert matches["actions/setup-python"] == (
        APPROVED_ACTIONS["actions/setup-python"],
        "v6.2.0",
    )


def test_workflow_has_read_only_token_and_does_not_persist_checkout_credentials():
    text = workflow_text()
    assert "permissions:\n  contents: read" in text
    assert "persist-credentials: false" in text
    assert "persist-credentials: true" not in text
    assert "contents: write" not in text
    assert "pull-requests: write" not in text
    assert "issues: write" not in text
    assert "deployments: write" not in text
    assert "actions: write" not in text


def test_python_setup_cache_is_bound_to_the_repository_requirements():
    text = workflow_text()
    assert 'python-version: "3.11"' in text
    assert "cache: pip" in text
    assert "cache-dependency-path: requirements.txt" in text
    assert "python -m pip install -r requirements.txt" in text
    assert "pip install -r requirements.txt" not in text.replace(
        "python -m pip install -r requirements.txt", ""
    )
    assert "pip install --upgrade pip" not in text
    assert "python -m pip install --upgrade pip" not in text


def test_ci_uses_selected_interpreter_for_compile_and_tests():
    text = workflow_text()
    assert (
        "python -m compileall -q ingestion transformations forecasting "
        "fabric/notebooks tests"
    ) in text
    assert "python -m pytest -q" in text
    assert "\n        run: pytest -q" not in text


def test_ci_has_bounded_runtime_and_obsolete_run_cancellation():
    text = workflow_text()
    assert "timeout-minutes: 20" in text
    assert "concurrency:" in text
    assert "group: ci-${{ github.workflow }}-${{ github.ref }}" in text
    assert "cancel-in-progress: true" in text


def test_supply_chain_document_records_the_same_action_identities():
    document = (ROOT / "CI_SUPPLY_CHAIN.md").read_text(encoding="utf-8")
    for action, commit in APPROVED_ACTIONS.items():
        assert action in document
        assert commit in document
    assert "Node 24" in document
    assert "persist-credentials: false" in document
