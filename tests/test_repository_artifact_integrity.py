from __future__ import annotations

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
STRICT_TEXT_EXTENSIONS = {
    ".sql",
    ".ipynb",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
}


def _tracked_paths() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [
        ROOT / item.decode("utf-8")
        for item in result.stdout.split(b"\0")
        if item
    ]


def test_tracked_source_and_document_artifacts_are_not_empty_placeholders():
    empty: list[str] = []
    for path in _tracked_paths():
        if not path.is_file():
            continue
        strict = path.suffix.lower() in STRICT_TEXT_EXTENSIONS
        python_source = path.suffix.lower() == ".py" and path.name != "__init__.py"
        if not (strict or python_source):
            continue
        if path.stat().st_size == 0 or not path.read_bytes().strip():
            empty.append(path.relative_to(ROOT).as_posix())
    assert empty == [], f"Tracked implementation artifacts must not be empty: {empty}"


def test_tracked_notebooks_are_valid_and_contain_at_least_one_cell():
    failures: list[str] = []
    for path in _tracked_paths():
        if path.suffix.lower() != ".ipynb":
            continue
        try:
            notebook = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            failures.append(f"{path.relative_to(ROOT)}: {exc}")
            continue
        if notebook.get("nbformat") != 4 or not notebook.get("cells"):
            failures.append(
                f"{path.relative_to(ROOT)}: expected nbformat 4 and non-empty cells"
            )
    assert failures == [], "Invalid tracked notebooks: " + "; ".join(failures)


def test_removed_silver_sql_placeholders_do_not_reappear():
    removed = [
        ROOT / "transformations/silver/clean_weather.sql",
        ROOT / "transformations/silver/clean_energy.sql",
        ROOT / "transformations/silver/deduplication.sql",
    ]
    assert not any(path.exists() for path in removed)


def test_silver_readme_declares_the_supported_implementation_boundary():
    readme = (
        ROOT / "transformations" / "silver" / "README.md"
    ).read_text(encoding="utf-8")
    assert "clean_weather.py" in readme
    assert "clean_energy.py" in readme
    assert "fabric/notebooks/02_bronze_to_silver.py" in readme
    assert "no second SQL implementation" in readme
    assert "pass-through analyst views only" in readme
