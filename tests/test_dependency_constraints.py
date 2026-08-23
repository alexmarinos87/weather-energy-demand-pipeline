from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "requirements.txt"
CONSTRAINTS = ROOT / "constraints" / "ci-python311-linux.txt"
PIN_PATTERN = re.compile(r"^([A-Za-z0-9_.-]+)==([^\s;]+)$")
EXPECTED_CONSTRAINT_PACKAGES = {
    "attrs",
    "certifi",
    "charset-normalizer",
    "idna",
    "iniconfig",
    "jsonschema",
    "jsonschema-specifications",
    "numpy",
    "packaging",
    "pandas",
    "pluggy",
    "pyarrow",
    "pytest",
    "python-dateutil",
    "pytz",
    "pyyaml",
    "referencing",
    "requests",
    "rpds-py",
    "six",
    "typing-extensions",
    "tzdata",
    "urllib3",
}


def canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def parse_pins(path: Path) -> dict[str, tuple[str, str]]:
    pins: dict[str, tuple[str, str]] = {}
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = PIN_PATTERN.fullmatch(line)
        assert match, f"{path}:{number} must be one exact name==version pin: {line!r}"
        raw_name, pinned_version = match.groups()
        name = canonical_name(raw_name)
        assert name not in pins, f"{path} contains duplicate canonical package {name!r}"
        pins[name] = (raw_name, pinned_version)
    assert pins, f"{path} must contain at least one exact pin"
    return pins


def test_direct_requirements_and_ci_constraints_are_exact_unique_pins():
    requirements = parse_pins(REQUIREMENTS)
    constraints = parse_pins(CONSTRAINTS)
    assert set(requirements).issubset(constraints)
    for name, (_, required_version) in requirements.items():
        assert constraints[name][1] == required_version


def test_ci_constraints_cover_the_reviewed_python311_linux_resolution():
    constraints = parse_pins(CONSTRAINTS)
    assert set(constraints) == EXPECTED_CONSTRAINT_PACKAGES
    direct = set(parse_pins(REQUIREMENTS))
    assert EXPECTED_CONSTRAINT_PACKAGES - direct == {
        "attrs",
        "iniconfig",
        "jsonschema-specifications",
        "numpy",
        "packaging",
        "pluggy",
        "python-dateutil",
        "pytz",
        "referencing",
        "rpds-py",
        "six",
        "typing-extensions",
        "tzdata",
    }


def test_running_ci_environment_matches_every_recorded_constraint():
    constraints = parse_pins(CONSTRAINTS)
    missing: list[str] = []
    mismatched: list[str] = []
    for name, (raw_name, expected_version) in constraints.items():
        try:
            installed_version = version(raw_name)
        except PackageNotFoundError:
            missing.append(name)
            continue
        if installed_version != expected_version:
            mismatched.append(
                f"{name}: expected {expected_version}, installed {installed_version}"
            )
    assert missing == [], f"Constrained packages are not installed: {missing}"
    assert mismatched == [], "CI environment diverged from constraints: " + "; ".join(mismatched)


def test_ci_installs_with_constraints_and_runs_pip_check():
    workflow = (
        ROOT / ".github" / "workflows" / "ci.yml"
    ).read_text(encoding="utf-8")
    assert (
        "python -m pip install -r requirements.txt "
        "-c constraints/ci-python311-linux.txt"
    ) in workflow
    assert "python -m pip check" in workflow
    cache_block = workflow.split("cache-dependency-path: |", 1)[1].split(
        "\n\n", 1
    )[0]
    assert "requirements.txt" in cache_block
    assert "constraints/ci-python311-linux.txt" in cache_block


def test_dependency_document_names_scope_and_refresh_boundary():
    document = (ROOT / "DEPENDENCY_REPRODUCIBILITY.md").read_text(
        encoding="utf-8"
    )
    assert "Python 3.11 and Ubuntu 24.04" in document
    assert "constraints/ci-python311-linux.txt" in document
    assert "python -m pip check" in document
    assert "reviewed PR" in document
    assert "does not pin pip, setuptools, or wheel" in document
